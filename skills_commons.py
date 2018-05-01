import sys
import urllib
import urlparse
import urllib2
import contextlib
import shutil
import os
import csv
import tempfile
import logging.config
import os.path
import xml.etree.ElementTree as ET
import textract
from zipfile import ZipFile, BadZipfile


BASE_URL = 'https://www.skillscommons.org'
SEARCH_URL = BASE_URL + '/discover?filtertype=type&filter_relational_operator=equals' + \
             '&filter=Online+Course&sort_by=dc.date.issued_dt&order=asc&rpp={}&page={}&XML'
SEARCH_RPP = 400
EXT_BLACKLIST = {'.png', '.gif', '.jpg', '.bmp',
                  '.swf', '.mp3', '.mp4', '.m4v', '.mpg', '.mov', '.wmv' 
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
    'language iso,'                  
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



log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
ch.setFormatter(formatter)
log.addHandler(ch)


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


def get_course_listing(rpp=SEARCH_RPP, max_res=sys.maxint, page=1):
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
# zips), it just globs the text together
def get_text_file(file_path):
    ret_text = None

    _, ext = os.path.splitext(file_path.lower())
    log.debug("\t\texamining {} ({})".format(file_path, ext))

    # if file_path.lower().endswith('.zip') or file_path.lower().endswith('.imscc'):
    if (ext == '.zip') or (ext == '.imscc'):
        zip_file_texts = ""
        log.debug("\t\tunzipping {} to {}".format(file_path, tempdir))
        with temp_dir() as tempdir, ZipFile(file_path, 'r') as zip:
            zip.extractall(tempdir)
            zip_file_paths = [ os.path.join(tempdir, fn) for fn in zip.namelist() ]

            for zip_file_path in zip_file_paths:
                log.debug("\t\trecursive get_text_file(" + zip_file_path + ")")
                text = get_text_file(zip_file_path)
                if text:
                    zip_file_texts += text + "\n"
        ret_text = zip_file_texts

    elif (ext in EXT_BLACKLIST) or (ext not in EXT_WHITELIST):
        tally_file_info(file_path)
        # log.debug("\t\tskipping (ext) {}".format(file_path))

    else:
        tally_file_info(file_path)
        log.debug("\t\textracting text from {}".format(file_path))
        try:
            ret_text = textract.process(file_path)
        except (textract.exceptions.ExtensionNotSupported, textract.exceptions.ShellError) as err:
            # log.debug("\t\tcan't extract file file {}".format(file_path))
            log.debug("\t\tcan't extract file file {}: {}".format(file_path, err))
            tally_file_errors(file_path)
        except (IOError, TypeError, BadZipfile) as err:
            log.debug("\t\tcan't extract file file {}: {}".format(file_path, err))
            tally_file_errors(file_path)
    return ret_text


def get_text_url(file_url):
    _, file_name = os.path.split(urlparse.urlsplit(file_url).path)
    _, ext = os.path.splitext(file_name.lower())
    if (ext in EXT_BLACKLIST) or (ext not in EXT_WHITELIST):
        log.debug("\t\tskipping (ext) {} => {}".format(file_url, file_name))
    else:
        log.debug("\t\tdownloading {} => {}".format(file_url, file_name))
        with temp_dir() as tempdir:
            url_fname, url_headers = urllib.urlretrieve(file_url, os.path.join(tempdir, file_name))
            text = get_text_file(url_fname)
    return text


def sorted_vals(dict_guy, reverse=False):
    return sorted(dict_guy.items(), key=lambda x: x[1], reverse=reverse)


def print_dict(dict_guy, reverse=False, form_func=lambda x: x):
    for k, v in sorted_vals(dict_guy, reverse=reverse):
        log.debug("{:30}  {}".format(k, form_func(v)))


def read_course_file(course_file_path):
    id__fields = {}
    log.debug("reading courses from {}".format(course_file_path))
    with open(course_file_path, 'rb') as course_file:
        course_reader = csv.reader(course_file, delimiter='\t', quotechar='"')
        for row in course_reader:
            course_id = row[0]
            course_fields = dict(zip(COURSE_FIELDS, row[1:]))
            id__fields[course_id] = course_fields
    log.debug("read fields for {} courses from {}".format(len(id__fields), course_file_path))
    return id__fields


def write_course_file(id__fields, course_file_path, append=True):
    write_count = 0
    with open(course_file_path, 'ab' if append else 'wb') as course_file:
        course_writer = csv.writer(course_file, delimiter='\t', quotechar='"',
                                   quoting=csv.QUOTE_MINIMAL)
        for id, fields in id__fields.items():
            course_writer.writerow([id] + [ fields.get(k, '') for k in COURSE_FIELDS ])
            write_count += 1
    log.debug("wrote {} course records to {}".format(write_count, course_file_path))
    return write_count


@contextlib.contextmanager
def temp_dir(*args, **kwargs):
    d = tempfile.mkdtemp(*args, **kwargs)
    try:
        yield d
    finally:
        shutil.rmtree(d)

# with temporary_directory() as temp_dir:

############################################

if __name__ == '__main__':

    # rpp = int(sys.argv[1])
    # max_results = int(sys.argv[2])
    data_dir = sys.argv[1]

    course_file_name = os.path.join(data_dir, 'courses.tsv')

    rpp = 200
    max_results = sys.maxint
    results = get_course_listing(rpp, max_results)
    print "got {} results".format(len(results))

    for i, (course_id, course_fields, course_files) in enumerate(results):
        log.debug("course {}/{}".format(i, len(results) - 1))

        for j, (file_name, file_label, file_type, file_url) in enumerate(course_files):
                log.debug("course {}/{}  file {}/{}".format(i, len(results) - 1,
                                                            j, len(res_files) - 1))
                text = get_text_url(file_url)





        log.debug("\n")

        write_course_file(id__fields, course_file_path, append=True)




















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
    log.debug("field counts:")
    # print_dict(res_field__count, reverse=True)
    res_field__perc = { k:float(v)/len(results) for k, v in res_field__count.items() }
    print_dict(res_field__perc, reverse=True, form_func=lambda p: '{:.4f}'.format(p))

    # # file type counts
    # mimetype__count = {}
    # file_ext__count = {}
    # for res_id, res_fields, res_files in results:
    #     for file_name, file_label, file_type, file_url in res_files:
    #         mimetype__count[file_type] = mimetype__count.get(file_type, 0) + 1
    #         _, ext = os.path.splitext(file_name)
    #         file_ext__count[ext] = file_ext__count.get(ext, 0) + 1
    #
    # log.debug("mimetypes:")
    # print_dict(mimetype__count, reverse=True)



    log.debug("file exts:")
    print_dict(file_ext__count, reverse=True)


    file_err_percs = { k: float(file_ext__err_count.get(k, 0))/count
                       for k, count in file_ext__count.items() }
    print_dict(file_err_percs)


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












