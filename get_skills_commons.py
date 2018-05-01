import sys
import urllib2
import xml.etree.ElementTree as ET


BASE_URL = 'https://www.skillscommons.org'
SEARCH_URL = BASE_URL + '/discover?filtertype=type&filter_relational_operator=equals' + \
             '&filter=Online+Course&sort_by=dc.date.issued_dt&order=asc&rpp={}&page={}&XML'


def get_course_search_url(rpp, page):
    return SEARCH_URL.format(rpp, page)


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
        results.extend(get_course_listing(rpp, max_res, page + 1))

    return results


############################################
if __name__ == '__main__':

    rpp = int(sys.argv[1])          # results per page
    max_results = int(sys.argv[2])  # total results to grab

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


