from bs4 import BeautifulSoup
import zipfile, requests, tqdm


def download_subtitles():
    """Downloads english OpenSubtitles corpus"""
    def download_url(url, save_path, chunk_size=128):
        r = requests.get(url, stream=True)
        with open(save_path, 'wb') as fd:
            for chunk in r.iter_content(chunk_size=chunk_size):
                fd.write(chunk)
    download_url("http://opus.nlpl.eu/download.php?f=OpenSubtitles/v2018/raw/en.zip", "en.zip")


def get_xml_filepaths_from_zip(archive):
    """
    Returns a list of paths to all xml files within a zip archive
    """
    xml_files = []
    for file in archive.namelist():
        if file.endswith('.xml'):
            # archive.extract(file, 'destination_path')
            xml_files.append(file)
    print(len(xml_files))
    return xml_files

def get_n_tokens(path_to_xml, archive):
    """extracts the number of tokens from meta tag in opensubtitles data"""
    xml = archive.open(path_to_xml, 'r')
    soup = BeautifulSoup(xml, 'html.parser')
    return int(soup.tokens.get_text())

def parse_single_example(path_to_xml, archive):
    """
    converts an xml file containing subtitle data from opensubtitles.org to plaintext
    """
    xml = archive.open(path_to_xml, 'r')
    soup = BeautifulSoup(xml, 'html.parser')
    subtitles = soup("s")
    subs = []
    for each in subtitles:
        subs.append(each.get_text())
    seperator = ' '
    return remove_blank_lines(seperator.join(subs))

def remove_blank_lines(txt):
    return "\n".join([s for s in txt.split("\n") if s])

def save_to_txt_file(txt, out_name):
    textfile = open(f'{out_name}.txt', 'w')
    textfile.write(txt)
    textfile.close()

if __name__ == "__main__":
    download_subtitles()
    archive = zipfile.ZipFile("en.zip")
    xml_fps = get_xml_filepaths_from_zip(archive)
    for count, f in tqdm.tqdm(enumerate(xml_fps)):
        x = parse_single_example(f, archive)
        save_to_txt_file(x, f"out/subs_{count:07}")
