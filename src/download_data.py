from typing import Optional
import gzip
import os
import requests
import shutil
import tarfile
import zipfile

from filepaths import *


def download_and_unzip(input_url: str,
                       download_path: str,
                       output_path: Optional[str] = None,
                       requires_unzip: bool = False,
                       requires_untar: bool = False,
                       should_clean: bool = False,
                       chunk_size: int = 1024) -> bool:
    """
    Downloads a file or archive and optionally unzips/untars/copies it to a destination.

    Inputs:
        `input_url`: Where to download data from.
        `download_path`: An absolute filepath to download to.
        `output_path`: The final destination where the downloaded data should end up.
        `requires_unzip`: Should we unzip the file after downloading?
        `requires_untar`: Should we untar the file after downloading?
        `should_clean`: Should we delete the temporary downloaded file when finished?
        `chunk_size`: The chunk size for downloading.
    
    Returns:
        (bool) Whether the file was downloaded (it might be skipped if found).
    """
    # If the file already exists, do not re-download it.
    if os.path.exists(output_path):
        print(f"    {output_path} already downloaded, skipping.")
        return False

    # Otherwise, download to the file in chunks.
    print(f"    Downloading from {input_url}")
    r = requests.get(input_url, stream=True)
    with open(download_path, "wb") as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)
    # Optionally unzip the downloaded file.
    if requires_unzip:
        if output_path is None:
            raise ValueError('Unzipping requires an output_path destination.')
        with zipfile.ZipFile(download_path, "r") as zip_to_unzip:
            zip_to_unzip.extractall(output_path)
    # Optionally un-tar the downloaded file.
    elif requires_untar:
        if output_path is None:
            raise ValueError('Extracting a tar requires an output_path destination.')
        with tarfile.open(download_path) as tar:
            tar.extractall(output_path)
    # If the user didn't ask for unzip/untar, but specified a different output_path,
    # copy the downloaded file to there.
    elif output_path is not None and output_path != download_path:
        shutil.copy(download_path, output_path)
    # Finally, optionally clean up the downloaded temporary file.
    if should_clean and output_path != download_path:
        os.remove(download_path)
    return True


def download_pudl_data(zenodo_url):
    """
    Downloads the archived PUDL data release. The most recent version can be found at https://catalystcoop-pudl.readthedocs.io/en/latest/data_access.html#zenodo-archives
    Inputs:
        zenodo_url: the url to the .tgz file hosted on zenodo
    """

    # get the version number
    pudl_version = zenodo_url.split("/")[-1].replace(".tgz", "")

    # if the pudl data already exists, do not re-download
    if os.path.exists(f"{downloads_folder()}pudl"):
        with open(f"{downloads_folder()}pudl/pudl_version.txt", "r") as f:
            existing_version = f.readlines()[0].replace('\n', '')
        if pudl_version == existing_version:
            print("    PUDL data already downloaded")
        else:
            print("    Downloading new version of pudl")
            shutil.rmtree(f"{downloads_folder()}pudl")
            download_pudl(zenodo_url, pudl_version)
            download_updated_pudl_database(download=True)
    else:
        download_pudl(zenodo_url, pudl_version)
        download_updated_pudl_database(download=True)


def download_pudl(zenodo_url, pudl_version):
    r = requests.get(zenodo_url, params={"download": "1"}, stream=True)
    # specify parameters for progress bar
    total_size_in_bytes = int(r.headers.get("content-length", 0))
    block_size = 1024 * 1024 * 10  # 10 MB
    downloaded = 0
    with open(f"{downloads_folder()}pudl.tgz", "wb") as fd:
        for chunk in r.iter_content(chunk_size=block_size):
            print(
                f"    Downloading PUDL. Progress: {(round(downloaded/total_size_in_bytes*100,2))}%   \r",
                end="",
            )
            fd.write(chunk)
            downloaded += block_size

    # extract the tgz file
    print("    Extracting PUDL data...")
    with tarfile.open(f"{downloads_folder()}pudl.tgz") as tar:
        tar.extractall(f"{data_folder()}")

    # rename the extracted directory to pudl so that we don't have to update this for future versions
    os.rename(f"{data_folder()}{pudl_version}", f"{downloads_folder()}pudl")

    # add a version file
    with open(f"{downloads_folder()}pudl/pudl_version.txt", "w+") as v:
        v.write(pudl_version)

    # delete the downloaded tgz file
    os.remove(f"{downloads_folder()}pudl.tgz")

    print("    PUDL download complete")


def download_updated_pudl_database(download=True):
    """
    Downloaded the updated `pudl.sqlite` file from datasette.

    This is temporary until a new version of the data is published on zenodo
    """
    if download is True:
        print("    Downloading updated pudl.sqlite from Datasette...")
        # remove the existing file from zenodo
        os.remove(f"{downloads_folder()}pudl/pudl_data/sqlite/pudl.sqlite")

        r = requests.get("https://data.catalyst.coop/pudl.db", stream=True)
        with open(f"{downloads_folder()}pudl/pudl_data/sqlite/pudl.sqlite", "wb") as fd:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                fd.write(chunk)

    else:
        pass


def download_chalendar_files():
    """
    download_chalendar_files
    Download raw and cleaned files. Eventually we'll do our own processing to get our own version of chalendar,
    but still will be useful to use this raw file and compare to this cleaned file.

    TODO: download functions share a lot of code, could refactor
    """
    # if there is not yet a directory for egrid, make it
    os.makedirs(f"{downloads_folder()}eia930/chalendar", exist_ok=True)

    # download the cleaned and raw files
    urls = [
        "https://gridemissions.s3.us-east-2.amazonaws.com/EBA_elec.csv.gz",
        "https://gridemissions.s3.us-east-2.amazonaws.com/EBA_raw.csv.gz",
    ]
    for url in urls:
        filename = url.split("/")[-1].replace(".gz", "")
        # if the file already exists, do not re-download it
        if os.path.exists(f"{downloads_folder()}eia930/chalendar/{filename}"):
            print(f"    {filename} already downloaded")
        else:
            print(f"    Downloading {filename}")
            r = requests.get(url, stream=True)

            with open(f"{downloads_folder()}eia930/chalendar/{filename}.gz", "wb") as fd:
                for chunk in r.iter_content(chunk_size=1024):
                    fd.write(chunk)

            # Unzip
            with gzip.open(
                f"{downloads_folder()}eia930/chalendar/{filename}.gz", "rb"
            ) as f_in:
                with open(
                    f"{downloads_folder()}eia930/chalendar/{filename}", "wb"
                ) as f_out:
                    shutil.copyfileobj(f_in, f_out)
            os.remove(f"{downloads_folder()}eia930/chalendar/{filename}.gz")


def download_egrid_files(egrid_files_to_download):
    """
    Downloads the egrid excel files
    Inputs:
        egrid_files_to_download: a list of urls for the egrid excel files that you want to download
    """
    # if there is not yet a directory for egrid, make it
    if not os.path.exists(f"{downloads_folder()}egrid"):
        os.mkdir(f"{downloads_folder()}egrid")

    # download the egrid files
    for url in egrid_files_to_download:
        filename = url.split("/")[-1]
        # if the file already exists, do not re-download it
        if os.path.exists(f"{downloads_folder()}egrid/{filename}"):
            print(f"    {filename} already downloaded")
        else:
            print(f"    Downloading {filename}")
            r = requests.get(url, stream=True)

            with open(f"{downloads_folder()}egrid/{filename}", "wb") as fd:
                for chunk in r.iter_content(chunk_size=1024):
                    fd.write(chunk)


def download_eia930_data(years_to_download):
    """
    Downloads the six month csv files from the EIA-930 website
    Inputs:
        years_to_download: list of four-digit year numbers to download from EIA-930
    """
    # if there is not yet a directory for EIA-930, make it
    if not os.path.exists(f"{downloads_folder()}eia930"):
        os.mkdir(f"{downloads_folder()}eia930")

    # download the egrid files
    for year in years_to_download:
        for description in ["BALANCE", "INTERCHANGE"]:
            for months in ["Jan_Jun", "Jul_Dec"]:
                if os.path.exists(
                    f"{downloads_folder()}eia930/EIA930_{description}_{year}_{months}.csv"
                ):
                    print(f"    {description}_{year}_{months} data already downloaded")
                else:
                    print(f"    downloading {description}_{year}_{months} data")
                    r = requests.get(
                        f"https://www.eia.gov/electricity/gridmonitor/sixMonthFiles/EIA930_{description}_{year}_{months}.csv",
                        stream=True,
                    )

                    with open(
                        f"{downloads_folder()}eia930/EIA930_{description}_{year}_{months}.csv",
                        "wb",
                    ) as fd:
                        for chunk in r.iter_content(chunk_size=1024 * 1024):
                            fd.write(chunk)


def download_epa_psdc(psdc_url):
    """
    Downloads the EPA's Power Sector Data Crosswalk
    Check for new releases at https://github.com/USEPA/camd-eia-crosswalk
    Inputs:
        psdc_url: the url to the csv file hosted on github
    """
    # if there is not yet a directory for egrid, make it
    if not os.path.exists(f"{downloads_folder()}epa"):
        os.mkdir(f"{downloads_folder()}epa")

    filename = psdc_url.split("/")[-1]
    # if the file already exists, do not re-download it
    if os.path.exists(f"{downloads_folder()}epa/{filename}"):
        print(f"    {filename} already downloaded")
    else:
        print(f"    Downloading {filename}")
        r = requests.get(psdc_url, stream=True)

        with open(f"{downloads_folder()}epa/{filename}", "wb") as fd:
            for chunk in r.iter_content(chunk_size=1024):
                fd.write(chunk)


def download_raw_eia923(year):
    """
    Downloads the egrid excel files
    Inputs:
        egrid_files_to_download: a list of urls for the egrid excel files that you want to download
    """
    # if there is not yet a directory for egrid, make it
    if not os.path.exists(f"{downloads_folder()}eia923"):
        os.mkdir(f"{downloads_folder()}eia923")

    url = f"https://www.eia.gov/electricity/data/eia923/archive/xls/f923_{year}.zip"

    filename = url.split("/")[-1].split(".")[0]
    # if the file already exists, do not re-download it
    if os.path.exists(f"{downloads_folder()}eia923/{filename}"):
        print(f"    {year} EIA-923 already downloaded")
    else:
        print(f"    Downloading {year} EIA-923 data")
        r = requests.get(url, stream=True)

        with open(f"{downloads_folder()}eia923/{filename}.zip", "wb") as fd:
            for chunk in r.iter_content(chunk_size=1024):
                fd.write(chunk)

        # Unzip
        with zipfile.ZipFile(
            f"{downloads_folder()}eia923/{filename}.zip", "r"
        ) as zip_to_unzip:
            zip_to_unzip.extractall(f"{downloads_folder()}eia923/{filename}")
        os.remove(f"{downloads_folder()}eia923/{filename}.zip")


def download_raw_eia_906_920(year):
    """
    For years before 2008, the EIA releases Form 906 and 920 instead of 923.
    """
    if year < 2005 or year > 2007:
        raise NotImplementedError(f'We haven\'t tested downloading raw EIA-906/920 data for \'{year}\'.')
    output_folder = f'f906920_{year}'
    download_and_unzip(
        f'https://www.eia.gov/electricity/data/eia923/archive/xls/f906920_{year}.zip',
        downloads_folder(os.path.join('eia923', output_folder + '.zip')),
        downloads_folder(os.path.join('eia923', output_folder)),
        requires_unzip=True,
        should_clean=True)


def download_raw_eia860(year):
    """
    Downloads the egrid excel files
    Inputs:
        egrid_files_to_download: a list of urls for the egrid excel files that you want to download
    """
    # if there is not yet a directory for egrid, make it
    if not os.path.exists(f"{downloads_folder()}eia860"):
        os.mkdir(f"{downloads_folder()}eia860")

    url = f"https://www.eia.gov/electricity/data/eia860/xls/eia860{year}.zip"
    archive_url = (
        f"https://www.eia.gov/electricity/data/eia860/archive/xls/eia860{year}.zip"
    )

    filename = url.split("/")[-1].split(".")[0]
    # if the file already exists, do not re-download it
    if os.path.exists(f"{downloads_folder()}eia860/{filename}"):
        print(f"    {year} EIA-860 already downloaded")
    else:
        print(f"    Downloading {year} EIA-860 data")
        try:
            r = requests.get(url, stream=True)

            with open(f"{downloads_folder()}eia860/{filename}.zip", "wb") as fd:
                for chunk in r.iter_content(chunk_size=1024):
                    fd.write(chunk)

            # Unzip
            with zipfile.ZipFile(
                f"{downloads_folder()}eia860/{filename}.zip", "r"
            ) as zip_to_unzip:
                zip_to_unzip.extractall(f"{downloads_folder()}eia860/{filename}")
            os.remove(f"{downloads_folder()}eia860/{filename}.zip")
        except:
            r = requests.get(archive_url, stream=True)

            with open(f"{downloads_folder()}eia860/{filename}.zip", "wb") as fd:
                for chunk in r.iter_content(chunk_size=1024):
                    fd.write(chunk)

            # Unzip
            with zipfile.ZipFile(
                f"{downloads_folder()}eia860/{filename}.zip", "r"
            ) as zip_to_unzip:
                zip_to_unzip.extractall(f"{downloads_folder()}eia860/{filename}")
            os.remove(f"{downloads_folder()}eia860/{filename}.zip")


def format_raw_eia860(year: int):
    """
    Makes sure that a folder of raw EIA-860 data has filenames that are consistent
    with the rest of the data pipeline.
    """
    if year < 2009:
        raise NotImplementedError(f'We haven\'t implemented data cleaning for {year} yet.')

    raw_folder = f"{downloads_folder()}eia860/eia860{year}"

    # For 2009-2012, the filenames for EnviroEquip and EnviroAssoc are different.
    if year >= 2009 and year <= 2012:
        # 2009 uses two digit years.
        year_format = str(year)[-2:] if year == 2009 else str(year)
        file_format = 'xls' if year <= 2010 else 'xlsx'

        # For some dumb reason, the 2011 data release doesn't include a year in the filename.
        equip_name = 'EnviroEquip.{}'.format(file_format) if year == 2011 else 'EnviroEquipY{}.{}'.format(year_format, file_format)
        enviro_assoc_input_filename = os.path.join(raw_folder, 'EnviroAssocY{}.{}'.format(year_format, file_format))
        enviro_equip_input_filename = os.path.join(raw_folder, equip_name)
        if not os.path.exists(enviro_assoc_input_filename):
            raise FileNotFoundError(enviro_assoc_input_filename)
        if not os.path.exists(enviro_equip_input_filename):
            raise FileNotFoundError(enviro_equip_input_filename)
        
        # Copy to a new file with our expected naming convention.
        shutil.copy(enviro_assoc_input_filename,
                os.path.join(raw_folder, InputDataFilenames.EIA_860_ENVIRO_ASSOC_FILE_FMT.format(year)))
        shutil.copy(enviro_assoc_input_filename,
                os.path.join(raw_folder, InputDataFilenames.EIA_860_ENVIRO_EQUIP_FILE_FMT.format(year)))


def format_raw_eia923(year: int):
    """Makes sure that a folder of raw EIA-923 has files with consistent names."""
    if year < 2008:
        raise NotImplementedError(f'We haven\'t implemented data cleaning for {year} yet.')

    raw_folder = f"{downloads_folder()}eia923/f923_{year}"
    consistent_filename = os.path.join(raw_folder, InputDataFilenames.EIA_923_ENVIRONMENTAL_INFO_FMT.format(year))

    # Lots of annoying filename changes for each year.
    if year == 2008:
        base_filename = "SCHEDULE 3A 5A 8A 8B 8C 8D 8E 8F 2008.xlsm"
    elif year == 2009:
        base_filename = "SCHEDULE 3A 5A 8A 8B 8C 8D 8E 8F REVISED 2009 04112011.xls"
    elif year == 2010:
        base_filename = 'SCHEDULE 3A 5A 8A 8B 8C 8D 8E 8F 2010 on NOV 30 2011.xls'
    elif year == 2011:
        base_filename = 'EIA923_Schedule_8_PartsA-F_EnvData_' + str(year) + '{}.xlsx'
    elif year == 2013:
        base_filename = 'EIA923_Schedule_8_PartsA-D_EnvData_' + str(year) + '{}.xlsx'
    elif year == 2017:
        base_filename = 'EIA923_Schedule_8_Annual_Envir_Infor_' + str(year) + '{}.xlsx'
    else:
        base_filename = 'EIA923_Schedule_8_Annual_Environmental_Information_' + str(year) + '{}.xlsx'

    # Sometimes the filename ends in 'Final' instead of 'Final_Revision'.
    final_revision_filename = os.path.join(raw_folder, base_filename.format('_Final_Revision'))
    final_filename = os.path.join(raw_folder, base_filename.format('_Final'))

    if os.path.exists(final_revision_filename):
        # Only copy if the filename is different.
        if consistent_filename != final_revision_filename:
            shutil.copy(final_revision_filename, consistent_filename)
    elif os.path.exists(final_filename):
        # Only copy if the filename is different.
        if consistent_filename != final_filename:
            shutil.copy(final_filename, consistent_filename)
    else:
        raise NotImplementedError()


def check_required_files_raw_eia860(year: int):
    """
    Checks that all required files are present in an EIA-860 data folder.
    """
    raw_folder = f"{downloads_folder()}eia860/eia860{year}"
    if not os.path.exists(raw_folder):
        raise FileNotFoundError(f'The supplied EIA-860 data folder \'{raw_folder}\' does not exist.')

    enviro_assoc_file = InputDataFilenames.EIA_860_ENVIRO_ASSOC_FILE_FMT.format(year)
    enviro_equip_file = InputDataFilenames.EIA_860_ENVIRO_EQUIP_FILE_FMT.format(year)
    if not os.path.exists(os.path.join(raw_folder, enviro_assoc_file)):
        raise FileNotFoundError(f'EIA-860 for year {year} is missing {enviro_assoc_file}')
    if not os.path.exists(os.path.join(raw_folder, enviro_equip_file)):
        raise FileNotFoundError(f'EIA-860 for year {year} is missing {enviro_equip_file}')


def check_required_files_raw_eia923(year: int):
    """
    Checks that all required files are present in an EIA-923 data folder.
    """
    raw_folder = f"{downloads_folder()}eia923/f923_{year}"
    if not os.path.exists(raw_folder):
        raise FileNotFoundError(f'The supplied EIA-923 data folder \'{raw_folder}\' does not exist.')

    environmental_info_file = InputDataFilenames.EIA_923_ENVIRONMENTAL_INFO_FMT.format(year)
    if not os.path.exists(os.path.join(raw_folder, environmental_info_file)):
        raise FileNotFoundError(f'EIA-923 for year {year} is missing {environmental_info_file}')
