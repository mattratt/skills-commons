import sys
import argparse
import urllib
import urlparse
import urllib2
import contextlib
import shutil
import os
import os.path
import errno
import csv
import tempfile
import logging.config
import xml.etree.ElementTree as ET
import textract
from zipfile import ZipFile, BadZipfile
from unidecode import unidecode
from joblib import Parallel, delayed


# https://www.skillscommons.org/discover?filtertype=type&filter_relational_operator=equals&filter=Online+Course&sort_by=dc.date.issued_dt&order=asc&rpp={}&page={}&XML

BASE_URL = 'https://www.skillscommons.org'
SEARCH_URL = BASE_URL + '/discover?filtertype=type&filter_relational_operator=equals' + \
             '&filter={}&sort_by=dc.date.issued_dt&order=asc&rpp={}&page={}&XML'

SEARCH_RPP = 400
EXT_BLACKLIST = {'.png', '.gif', '.jpg', '.bmp', '.jpeg',
                  '.swf', '.mp3', '.mp4', '.m4v', '.mpg', '.mov', '.wmv', '.wav',
                  '.dll', '.exe',
                  '.thmx', '.accdb',
                  '.xml', '.css', '.js', '.cpp', '.jar'}
EXT_WHITELIST = {'.csv', '.doc', '.docx', '.eml', '.epub', '.gif', '.htm', '.html',
                 '.jpeg', '.jpg', '.json', '.log', '.mp3', '.msg', '.odt', '.ogg',
                 '.pdf', '.png', '.pptx', '.ps', '.psv', '.rtf', '.tff', '.tif',
                 '.tiff', '.tsv', '.txt', '.wav', '.xls', '.xlsx',
                 '.zip', '.imscc'}  # all but the last two come from textract; the zips we handle

COURSE_FIELDS = [
    'identifier uri',
    'date available',
    'description abstract',
    'creditType',
    'interactivityType',
    'title',
    'description sponsorship',
    'credentialType',
    'type',
    'date accessioned',
    'date issued',
    'publisher',
    'instructional',
    'license',
    'level',
    'industry',
    'occupation',
    'language iso'                  
    'contributor author',
    'round',
    'ada textAdjustmentCompatible',
    'OCDQuality',
    'timeRequired',
    'SMQuality',
    'subject',
    'ada hyperlinkActive',
    'quality',
    'ada contrast',
    'ada color',
    'ada textAdjustable',
    'ada readingOrder',
    'ada noFlickering',
    'ada structuralMarkupText',
    'ada structuralMarkupLists',
    'ada textAccess',
    'ada languageMarkup',
    'qualityNote',
    'type secondary',
    'ada decorativeImages',
    'ada tableMarkup',
    'ada multimediaTranscript',
    'ada imageAltText',
    'ada multimediaTextTrack',
    'ada multimediaAccessiblePlayer',
    'ada stemMarkup',
    'ada complextImageText',
    'ada stemNotationMarkup',
    'ada interactiveMarkup',
    'ada keyboardInteractive',
    'ada interactivePromptText',
    'ada readingLayoutCompatible',
    'projectName',
    'courseNote',
    'ada statement',
    'ada readingLayoutPageNumbers',
    'ada readingLayoutPageNumbersAlt',
    'materialsReuse',
    'ada languageMarkupAlt',
    'ccby',
    'ada formalPolicy',
    'ada structuralMarkupReaders',
    'date updated',
    'license secondary',
    'archive',
    'rightsHolder',
    'ada organization',
    'object uri',
]


def get_log(nm):
    log = logging.getLogger(nm)
    log.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    ch.setFormatter(formatter)
    log.addHandler(ch)
    return log
log = get_log(__name__)


def get_course_search_url(course_type, rpp, page):
    if course_type.lower() == 'online':
        typ = 'Online+Course'
    elif course_type.lower() in {'blended', 'hybrid'}:
        typ = 'Hybrid%2FBlended+Course'
    else:
        raise Exception("Bad course type: {} (must be 'online' or 'blended')".format(course_type))

    return SEARCH_URL.format(typ, rpp, page)


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


def get_course_listing(course_type, rpp=SEARCH_RPP, max_res=sys.maxint, page=1):
    url = get_course_search_url(course_type, rpp, page)
    print url
    response = urllib2.urlopen(url)
    xml_raw = response.read()
    xml_parsed = ET.fromstring(xml_raw)

    xml_results = xml_parsed.find('alchemy/results')

    results = []  # (id, fields { name->val }, files [ (name, lab, type, url) ])
    for xml_result in xml_results.findall('{http://www.loc.gov/METS/}METS'):
        res_id = xml_result.attrib['ID']

        res_fields = get_fields(xml_result, '{http://www.dspace.org/xmlns/dspace/dim}field')

        res_files = []
        xml_filesec = xml_result.find('{http://www.loc.gov/METS/}fileSec')
        for xml_filegrp in xml_filesec:
            for xml_file in xml_filegrp.findall('{http://www.loc.gov/METS/}file'):
                file_type = xml_file.attrib.get('MIMETYPE')

                xml_file_loc = xml_file.find('{http://www.loc.gov/METS/}FLocat')

                file_name = xml_file_loc.attrib.get('{http://www.w3.org/TR/xlink/}title')
                file_label = xml_file_loc.attrib.get('{http://www.w3.org/TR/xlink/}label')
                file_url = BASE_URL + xml_file_loc.attrib.get('{http://www.w3.org/TR/xlink/}href')
                res_files.append((file_name, file_label, file_type, file_url))

        results.append((res_id, res_fields, res_files))

    res_end = int(xml_results.find('end').text)
    res_tot = int(xml_results.find('total').text)
    if (res_end < res_tot) and (res_end < max_res):
        print "got {} results (up to {}/{}), getting more".format(len(results), res_end, res_tot)
        results.extend(get_course_listing(course_type, rpp, max_res, page + 1))

    return results[:max_res]


file_ext__count = {}
def tally_file_info(file_name):
    global file_ext__count
    _, ext = os.path.splitext(file_name)
    file_ext__count[ext] = file_ext__count.get(ext, 0) + 1


file_ext__err_count = {}
def tally_file_errors(file_name):
    global file_ext__err_count
    _, ext = os.path.splitext(file_name)
    file_ext__err_count[ext] = file_ext__err_count.get(ext, 0) + 1


# this takes text out of as many files as it can --- in the case of a zip (or zip full of
# zips), it collects the text from each sub-file into a dict
def get_text_file(file_dir, file_name):
    file_path = os.path.join(file_dir, file_name)
    filepath__text = {}

    _, ext = os.path.splitext(file_path.lower())
    if (ext == '.zip') or (ext == '.imscc'):
        try:
            with temp_dir() as tempdir, ZipFile(file_path, 'r') as zip:

                text_file_names = []
                for zfn in zip.namelist():
                    _, zext = os.path.splitext(zfn.lower())

                    if (zext in EXT_BLACKLIST) or (zext not in EXT_WHITELIST):
                        filepath__text[zfn] = None
                    else:
                        text_file_names.append(zfn)

                log.debug("\t\tunzipping {} to {}".format(file_path, tempdir))
                zip.extractall(tempdir, text_file_names)

                for fn in text_file_names:
                    filepath__text.update(get_text_file(tempdir, fn))

        except (BadZipfile, IOError, UnicodeEncodeError) as err:
            tally_file_info(file_path)
            filepath__text[file_name] = None

    elif (ext in EXT_BLACKLIST) or (ext not in EXT_WHITELIST):
        tally_file_info(file_path)
        filepath__text[file_name] = None

    else:
        tally_file_info(file_path)
        ret_text = None
        try:
            ret_text = textract.process(file_path)
            ret_text = ' '.join(ret_text.split())
        except Exception as err:
            log.debug("\t\tcan't extract file file {}: {}".format(file_path, err))
            tally_file_errors(file_path)

        filepath__text[file_name] = ret_text

    return filepath__text


def get_text_url(file_url):
    _, file_name = os.path.split(urlparse.urlsplit(file_url).path)
    _, ext = os.path.splitext(file_name.lower())
    if (ext in EXT_BLACKLIST) or (ext not in EXT_WHITELIST):
        log.debug("\t\tskipping (ext) {} => {}".format(file_url, file_name))
        filepath__text = { file_name: None }
    else:
        log.debug("\t\tdownloading {} => {}".format(file_url, file_name))
        with temp_dir() as tempdir:
            url_fname, url_headers = urllib.urlretrieve(file_url, os.path.join(tempdir, file_name))
            filepath__text = get_text_file(tempdir, file_name)
    return filepath__text


def sorted_vals(dict_guy, reverse=False):
    return sorted(dict_guy.items(), key=lambda x: x[1], reverse=reverse)


def print_dict(dict_guy, reverse=False, form_func=lambda x: x):
    for k, v in sorted_vals(dict_guy, reverse=reverse):
        log.debug("{:30}  {}".format(k, form_func(v)))


def read_course_info(course_file_path):
    id__fields = {}
    log.debug("reading courses from {}".format(course_file_path))
    if os.path.exists(course_file_path):
        with open(course_file_path, 'rb') as course_file:
            course_reader = csv.reader(course_file, delimiter='\t', quotechar='"')
            for row in course_reader:
                course_id = row[0]
                course_fields = dict(zip(COURSE_FIELDS, row[1:]))
                id__fields[course_id] = course_fields
        log.debug("read fields for {} courses from {}".format(len(id__fields), course_file_path))
    return id__fields


def write_course_infos(id__fields, course_file_path, append=True):
    rows = []
    for fid, fields in id__fields.items():
        row = [fid]
        for k in COURSE_FIELDS:
            val = fields.get(k)
            if val is None:
                row.append('')
            else:
                row.append(' '.join(val.split()))
        rows.append(row)

    return write_records(rows, course_file_path, append)


def write_file_infos(course_id, file_name__text, out_path, append=True):
    rows = []
    for file_name, text in file_name__text.items():
        _, ext = os.path.splitext(file_name.lower())
        rows.append([course_id, file_name, ext, '' if text is None else text ])
    write_records(rows, out_path, append)


def write_records(rows, out_file_path, append=True):
    write_count = 0
    with open(out_file_path, 'ab' if append else 'wb') as out_file:
        out_writer = csv.writer(out_file, delimiter='\t', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for row in rows:
            # out_writer.writerow([r.encode("utf-8") for r in row])
            try:
                out_writer.writerow(row)
            except (UnicodeEncodeError, UnicodeDecodeError) as err:
                # out_writer.writerow([ r.encode('ascii', 'ignore') for r in row ])
                try:
                    out_writer.writerow([ unidecode(r) for r in row ])
                except Exception as err2:
                    log.debug("error writing row")
                    continue

            write_count += 1
    log.debug("wrote {} records to {}".format(write_count, out_file_path))
    return write_count


@contextlib.contextmanager
def temp_dir(*args, **kwargs):
    d = tempfile.mkdtemp(*args, **kwargs)
    try:
        yield d
    finally:
        shutil.rmtree(d)


#https://stackoverflow.com/questions/10840533/most-pythonic-way-to-delete-a-file-which-may-not-exist
def remove_file(filename):
    try:
        os.remove(filename)
    except OSError as e: # this would be "except OSError, e:" before Python 2.6
        if e.errno != errno.ENOENT: # errno.ENOENT = no such file or directory
            raise # re-raise exception if a different error occurred


def chunks(lst, chunk_size):
    for i in xrange(0, len(lst), chunk_size):
        yield lst[i:i+chunk_size]


############################################

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Pull down course info from Skills Commons')
    parser.add_argument('datadir', help='destination for tsv files produced')
    parser.add_argument('--max_results', type=int, default=sys.maxint,
                        help='max number of courses to download')
    parser.add_argument('--rpp', type=int, default=300,
                        help='results per page for Skills Commons API')
    parser.add_argument('--cont', action='store_true',
                        help='continue (skip courses already proessed)')
    args = parser.parse_args()

    course_info_path = os.path.join(args.datadir, 'courses.tsv')
    file_info_path = os.path.join(args.datadir, 'files.tsv')

    results = get_course_listing('online', args.rpp, args.max_results)
    print "got {} results".format(len(results))

    if 0:
        # file type counts
        mimetype__count = {}
        file_ext__count = {}
        file_count = 0
        for res_id, res_fields, res_files in results:
            file_count += len(res_files)
            for file_name, file_label, file_type, file_url in res_files:
                mimetype__count[file_type] = mimetype__count.get(file_type, 0) + 1
                _, ext = os.path.splitext(file_name)
                file_ext__count[ext] = file_ext__count.get(ext, 0) + 1

        log.debug("mimetypes:")
        print_dict(mimetype__count, reverse=True)
        log.debug(" ")
        log.debug("{} files".format(file_count))
        log.debug("file exts:")
        for ext in file_ext__count.keys():
            file_ext__count[ext] = float(file_ext__count[ext])/file_count
        print_dict(file_ext__count, reverse=True)

        sys.exit()

    if args.cont:
        processed = read_course_info(course_info_path)
    else:
        remove_file(course_info_path)
        remove_file(file_info_path)
        processed = dict()  # checking and empty dict repeatedly is admittedly a little ugly

    for i, (course_id, course_fields, course_files) in enumerate(results):
        log.debug("course {}/{}".format(i, len(results) - 1))

        if course_id in processed:
            log.debug("skipping previously processed course")
            continue

        file_name__text = {}
        for j, (file_name, file_label, file_type, file_url) in enumerate(course_files):
            log.debug("course {}/{}  file {}/{}".format(i, len(results) - 1,
                                                        j, len(course_files) - 1))
            file_name__text.update(get_text_url(file_url))

        for file_name, text in sorted(file_name__text.items()):
            try:
                log.debug("\t{} ({})".format(file_name.encode('ascii', 'ignore'),
                                             len(text) if text is not None else 0))
            except Exception as err:
                continue

        write_course_infos({ course_id: course_fields }, course_info_path, append=True)
        write_file_infos(course_id, file_name__text, file_info_path, append=True)

        log.debug("\n")








