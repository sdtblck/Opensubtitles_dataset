from bs4 import BeautifulSoup
import zipfile, requests, tqdm, os
from archiver import Archive

def download_subtitles(language='en'):
    """Downloads OpenSubtitles corpus for a specific language"""
    def download_url(url, fname):
        resp = requests.get(url, stream=True)
        total = int(resp.headers.get('content-length', 0))
        with open(fname, 'wb') as file, tqdm.tqdm(
                desc=fname,
                total=total,
                unit='iB',
                unit_scale=True,
                unit_divisor=1024,
        ) as bar:
            for data in resp.iter_content(chunk_size=1024):
                size = file.write(data)
                bar.update(size)
    download_url(f"http://opus.nlpl.eu/download.php?f=OpenSubtitles/v2018/raw/{language}.zip", f"{language}.zip")


def get_xml_filepaths_from_zip(archive):
    """
    Returns a list of paths to all xml files within a zip archive
    """
    xml_files = []
    for file in archive.namelist():
        if file.endswith('.xml'):
            # archive.extract(file, 'destination_path')
            xml_files.append(file)
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
        l = each.get_text()
        l = l.replace('\n', '')
        l = '"' + l.replace('\t', '').strip().strip('-').strip('/') + '"'
        subs.append(l)
    seperator = ' '
    return remove_blank_lines(seperator.join(subs))

def remove_blank_lines(txt):
    return "\n".join([s for s in txt.split("\n") if s])

def save_to_txt_file(txt, out_name):
    textfile = open(f'{out_name}.txt', 'w')
    textfile.write(txt)
    textfile.close()

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


if __name__ == "__main__":
    language = 'en'
    download_subtitles(language)
    z = zipfile.ZipFile(f"{language}.zip")
    xml_fps = get_xml_filepaths_from_zip(z)
    n_files = len(xml_fps)
    xml_fps_chunked = chunks(xml_fps, int(n_files/10))
    try:
        os.mkdir('out')
    except:
        pass
    archive = Archive('out')
    for chunk in xml_fps_chunked:
        for count, f in tqdm.tqdm(enumerate(chunk), total=len(chunk)):
            x = parse_single_example(f, z)
            archive.add_data(x)
        archive.commit()
