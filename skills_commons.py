import sys
import urllib
import urlparse
import urllib2
import shutil
import os
import tempfile
import logging
import os.path
import xml.etree.ElementTree as ET
import textract
from zipfile import ZipFile


logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")


BASE_URL = 'https://www.skillscommons.org'
SEARCH_URL = BASE_URL + '/discover?filtertype=type&filter_relational_operator=equals' + \
             '&filter=Online+Course&sort_by=dc.date.issued_dt&order=asc&rpp={}&page={}&XML'
SEARCH_RPP = 400


def get_course_search_url(rpp, page):
    return SEARCH_URL.format(rpp, page)


def print_children_tags(xml_elt):
    for child in xml_elt:
        print child.tag , child.attrib


def print_tag_tree(xml_elt, indents=0):
    for child in xml_elt:
        print "\t"*indents, child.tag #, xml_elt.attrib
        print_tag_tree(child, indents+1)


def get_fields(xml_elt, field_tag='field'):
    fields = {}
    for i, field_elt in enumerate(xml_elt.iter(field_tag)):
        field_name = field_elt.attrib['element']
        if 'qualifier' in field_elt.attrib:
            field_name += " " + field_elt.attrib['qualifier']
        field_val = field_elt.text
        fields[field_name] = field_val
    return fields


def get_course_listing(rpp, max_res=sys.maxint, page=1):
    url = get_course_search_url(rpp, page)
    print url
    response = urllib2.urlopen(url)
    xml_raw = response.read()
    xml_parsed = ET.fromstring(xml_raw)
    # print_tag_tree(xml_parsed)
    # print_children_tags(xml_parsed)

    xml_results = xml_parsed.find('alchemy/results')
    # print_children_tags(xml_results)

    results = []  # (id, fields { name->val }, files [ (name, lab, type, url) ])
    for xml_result in xml_results.findall('{http://www.loc.gov/METS/}METS'):
        res_id = xml_result.attrib['ID']

        res_fields = get_fields(xml_result, '{http://www.dspace.org/xmlns/dspace/dim}field')

        res_files = []
        xml_filesec = xml_result.find('{http://www.loc.gov/METS/}fileSec')
        for xml_filegrp in xml_filesec:
            # print xml_filegrp.tag, xml_filegrp.attrib
            for xml_file in xml_filegrp.findall('{http://www.loc.gov/METS/}file'):
                file_type = xml_file.attrib.get('MIMETYPE')

                xml_file_loc = xml_file.find('{http://www.loc.gov/METS/}FLocat')
                # print "FILE LOC: ", xml_file_loc.tag, xml_file_loc.attrib

                file_name = xml_file_loc.attrib.get('{http://www.w3.org/TR/xlink/}title')
                file_label = xml_file_loc.attrib.get('{http://www.w3.org/TR/xlink/}label')
                file_url = BASE_URL + xml_file_loc.attrib.get('{http://www.w3.org/TR/xlink/}href')
                res_files.append((file_name, file_label, file_type, file_url))

        results.append((res_id, res_fields, res_files))

    res_end = int(xml_results.find('end').text)
    res_tot = int(xml_results.find('total').text)
    if (res_end < res_tot) and (res_end < max_res):
        print "got {} results (up to {}/{}), getting more".format(len(results), res_end, res_tot)
        results.extend(get_course_listing(rpp, max_res, page + 1))

    return results


def get_file_text(file_name, file_url, working_dir='temp_dir'):
    if not os.path.exists(working_dir):
        os.makedirs(working_dir)

    file_path_local = os.path.join(working_dir, file_name)
    print "downloading {} => {}".format(file_url, file_path_local)
    fname, headers = urllib.urlretrieve(file_url, file_path_local)
    # print "got {} with headers {}".format(fname, headers)

    if file_name.endswith('.zip') or file_name.endswith('.imscc:'):
        with ZipFile(file_path_local, 'r') as zip:
            zip.extractall(working_dir)
            file_paths = [ os.path.join(working_dir, fn) for fn in zip.namelist() ]
    else:
        file_paths = [fname]

    print "extracting text from", "\n\t".join(file_paths)
    file_text = ""
    for fp in file_paths:
        try:
            file_text += textract.process(fp) + "\n"
        except (textract.exceptions.ExtensionNotSupported, textract.exceptions.ShellError) as err:
            print "can't extract file file {}".format(fp)
            continue
        os.remove(fp)
    return file_text


# this takes text out of as many files as it can --- in the case of a zip (or zip full of
# zips), it just globs the text together
def get_text_file(file_path):
    ret_text = None
    if file_path.lower().endswith('.zip') or file_path.lower().endswith('.imscc:'):
        zip_file_texts = ""
        tempdir = tempfile.mkdtemp()
        logging.debug("unzipping {} to {}".format(file_path, tempdir))
        with ZipFile(file_path, 'r') as zip:
            zip.extractall(tempdir)
            zip_file_paths = [ os.path.join(tempdir, fn) for fn in zip.namelist() ]

            for zip_file_path in zip_file_paths:
                zip_file_texts += get_text_file(zip_file_path) + "\n"
        shutil.rmtree(tempdir)
        ret_text = zip_file_texts

    else:
        try:
            logging.debug("extracting text from {}".format(file_path))
            ret_text = textract.process(file_path)
        except (textract.exceptions.ExtensionNotSupported, textract.exceptions.ShellError) as err:
            logging.debug("can't extract file file {}".format(file_path))

    return ret_text


def get_text_url(file_url):
    tempdir = tempfile.mkdtemp()
    _, file_name = os.path.split(urlparse.urlsplit(file_url).path)

    logging.debug("downloading {} => {}".format(file_url, file_name))
    url_fname, url_headers = urllib.urlretrieve(file_url, os.path.join(tempdir, file_name))
    text = get_text_file(url_fname)
    shutil.rmtree(tempdir)
    return text


# def unzip(zip_path, working_dir='.'):
#     with ZipFile(zip_path, 'r') as zip:
#         zip.extractall(working_dir)
#
#         file_paths = [os.path.join(working_dir, fn) for fn in zip.namelist()]
#
#
# # https://stackoverflow.com/questions/9816816/get-absolute-paths-of-all-files-in-a-directory
# def absoluteFilePaths(directory):
#    for dirpath,_,filenames in os.walk(directory):
#        for f in filenames:
#            yield os.path.abspath(os.path.join(dirpath, f))
#


def sorted_vals(dict_guy, reverse=False):
    return sorted(dict_guy.items(), key=lambda x: x[1], reverse=reverse)


def print_dict(dict_guy, reverse=False):
    for k, v in sorted_vals(dict_guy, reverse=reverse):
        logging.debug("{:30}  {}".format(k, v))


############################################

if __name__ == '__main__':

    rpp = int(sys.argv[1])
    max_results = int(sys.argv[2])

    results = get_course_listing(rpp, max_results)
    print "got {} results".format(len(results))

    for res_id, res_fields, res_files in results[:5]:
        print "COURSE", res_id
        for field, val in sorted(res_fields.items()):
            print "\t", field, "->", val
        print "\tfiles:"
        for file_tup in res_files:
            print "\t\t", file_tup
        print "\n"

    # find which results fields keys are present
    res_field__count = {}
    for res_id, res_fields, res_files in results:
        for key, val in res_fields.items():
            if val is not None:
                res_field__count[key] = res_field__count.get(key, 0) + 1
    logging.debug("field counts:")
    print_dict(res_field__count, reverse=True)

    # file type counts
    mimetype__count = {}
    file_ext__count = {}
    for res_id, res_fields, res_files in results:
        for file_name, file_label, file_type, file_url in res_files:
            mimetype__count[file_type] = mimetype__count.get(file_type, 0) + 1
            _, ext = os.path.splitext(file_name)
            file_ext__count[ext] = file_ext__count.get(ext, 0) + 1

    logging.debug("mimetypes:")
    print_dict(mimetype__count, reverse=True)

    logging.debug("file exts:")
    print_dict(file_ext__count, reverse=True)



    #
    # mimetype__count = {}
    # for res_id, res_fields, res_files in results:
    #     for file_name, file_label, file_type, file_url in res_files:
    #         mimetype__count[file_type] = mimetype__count.get(file_type, 0) + 1
    #
    #
    #         print "FILE:", file_name, file_type, file_url
    #         text = get_file_text(file_name, file_url)
    #         print "TEXT:", text[:500]
    #         print "\n"
    #
    #
    #
    # for mimetype, count in sorted(mimetype__count.items(), key=lambda x: x[1], reverse=True):
    #     print mimetype, count












