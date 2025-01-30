from __future__ import unicode_literals, division, print_function

# built in modules
import argparse
import codecs
import os

from charset_normalizer.utils import identify_sig_or_bom
from six.moves import input
import shutil
import sys
import time
import requests

try:
    from unidecode import unidecode
except ImportError:
    pass


# third party-dependencies
import spacy


SNOMED_SEMANTIC_TYPES = [
    'OWL metadata concept',
    'administration method',
    'assessment scale',
    'attribute',
    'basic dose form',
    'body structure',
    'cell structure',
    'cell',
    'clinical drug',
    'core metadata concept',
    'disorder',
    'disposition',
    'dose form',
    'environment / location',
    'environment',
    'ethnic group',
    'event',
    'finding',
    'foundation metadata concept',
    'geographic location',
    'inactive concept',
    'intended site',
    'life style',
    'link assertion',
    'linkage concept',
    'medicinal product form',
    'medicinal product',
    'metadata',
    'morphologic abnormality',
    'namespace concept',
    'navigational concept',
    'number',
    'observable entity',
    'occupation',
    'organism',
    'person',
    'physical force',
    'physical object',
    'procedure',
    'product name',
    'product',
    'qualifier value',
    'racial group',
    'record artifact',
    'regime/therapy',
    'release characteristic',
    'religion/philosophy',
    'role',
    'situation',
    'social concept',
    'special concept',
    'specimen',
    'staging scale',
    'staging scales',
    'state of matter',
    'substance',
    'supplier',
    'transformation',
    'tumor staging',
    'unit of presentation',
]

# based on ISO 639-1 standard language codes
FHIR_LANGUAGES = {
    'BAQ': 'eu',           # Basque
    'CHI': 'zh',           # Chinese
    'CZE': 'cs',           # Czech
    'DAN': 'da',           # Danish
    'DUT': 'nl',           # Dutch
    'ENG': 'en',           # English
    'EST': 'et',           # Estonian
    'FIN': 'fi',           # Finnish
    'FRE': 'fr',           # French
    'GER': 'de',           # German
    'GRE': 'el',           # Greek
    'HEB': 'he',           # Hebrew
    'HUN': 'hu',           # Hungarian
    'ITA': 'it',           # Italian
    'JPN': 'ja',           # Japanese
    'KOR': 'ko',           # Korean
    'LAV': 'lv',           # Latvian
    'NOR': 'no',           # Norwegian
    'POL': 'pl',           # Polish
    'POR': 'pt',           # Portuguese
    'RUS': 'ru',           # Russian
    'SCR': 'hr',           # Croatian
    'SPA': 'es',           # Spanish
    'SWE': 'sv',           # Swedish
    'TUR': 'tr',           # Turkish
}

# project modules
from .toolbox import countlines, CuiSemTypesDB, SimstringDBWriter, mkdir
from .constants import HEADERS_MRCONSO, HEADERS_MRSTY, LANGUAGES, SPACY_LANGUAGE_MAP

def process_concept(concept, opts):
    code = concept['code']
    system = concept['system']
    preferred_name = concept['display'].strip()
    synonyms = {preferred_name}
    semantic_type = opts.semantic
    if 'designation' in concept:
        for designation in concept['designation']:
            if designation['use']['system'] == "http://snomed.info/sct":
                if designation['use']['code'] == "900000000000013009":
                    # synonym
                    synonyms.add(designation['value'].strip())
                elif designation['use']['code'] == "900000000000003001":
                    # fully specified name
                    if system == "http://snomed.info/sct":
                        # determine semantic type
                        for type in SNOMED_SEMANTIC_TYPES:
                            if designation['value'].endswith('(' + type + ')'):
                                semantic_type = type
                                synonyms.add(designation['value'][:-(len(type) + 2)].strip())
                                break
                    else:
                        synonyms.add(designation['value'].strip())
    for synonym in synonyms:
        if opts.lowercase:
            synonym = synonym.lower()

        if opts.normalize_unicode:
            synonym = unidecode(synonym)
        yield synonym, code, [semantic_type], synonym == preferred_name


def extract_from_fhir(opts):

    start = time.time()

    fhir_lang = FHIR_LANGUAGES[opts.language] if opts.language in FHIR_LANGUAGES else 'en'

    base_request_params = {
        'displayLanguage': fhir_lang,
        'includeDesignations': 'true',
        'activeOnly':  'true',
        'url': opts.valueset_url
    }

    count = 100
    offset = 0

    request_params = {
        'count': count,
        'offset':offset,
        **base_request_params
    }
    add_slash = '' if opts.fhir_server.endswith('/') else '/'
    request_url = opts.fhir_server + add_slash + 'ValueSet/$expand?'
    session = requests.Session()
    r = session.get(request_url, params=request_params)
    result_json = r.json()
    for concept in result_json['expansion']['contains']:
        yield from process_concept(concept, opts)

    number_found = result_json['expansion']['total']
    offset += count
    while offset < number_found:
        request_params = {
            'count': count,
            'offset': offset,
            **base_request_params
        }
        r = session.get(request_url, params=request_params)
        result_json = r.json()
        for concept in result_json['expansion']['contains']:
                yield from process_concept(concept, opts)
        offset += count
    session.close()
    delta = time.time() - start
    status = '\nCOMPLETED: {:,} in {:.2f} s '.format(number_found, delta)
    print(status)




def parse_and_encode_ngrams(extracted_it, simstring_dir, cuisty_dir, database_backend):
    # Create destination directories for the two databases
    mkdir(simstring_dir)
    mkdir(cuisty_dir)

    ss_db = SimstringDBWriter(simstring_dir)
    cuisty_db = CuiSemTypesDB(cuisty_dir, database_backend=database_backend)

    simstring_terms = set()

    for i, (term, cui, stys, preferred) in enumerate(extracted_it, start=1):
        if term not in simstring_terms:
            ss_db.insert(term)
            simstring_terms.add(term)

        print(term, cui, stys, preferred)
        cuisty_db.insert(term, cui, stys, preferred)


def install_spacy(lang):
    """Tries to create a spacy object; if it fails, downloads the dataset"""

    print(f'Determining if SpaCy for language "{lang}" is installed...')

    if lang in SPACY_LANGUAGE_MAP:
        try:
            spacy.load(SPACY_LANGUAGE_MAP[lang])
            print(f'SpaCy is installed and avaliable for {lang}!')
        except OSError:
            print(f'SpaCy is not available! Attempting to download and install...')
            spacy.cli.download(SPACY_LANGUAGE_MAP[lang])


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        'valueset_url',
        help=('Valueset url to extract data from.')
    )
    ap.add_argument(
        'destination_path',
        help='Location where the necessary QuickUMLS files are installed'
    )
    ap.add_argument(
        '-F', '--fhir-server', action='store_true', default='https://tx.ontoserver.csiro.au/fhir',
        help='Fhir server to retrieve data from.'
    )
    ap.add_argument(
        '-L', '--lowercase', action='store_true',
        help='Consider only lowercase version of tokens'
    )
    ap.add_argument(
        '-U', '--normalize-unicode', action='store_true',
        help='Normalize unicode strings to their closest ASCII representation'
    )
    ap.add_argument(
        '-d', '--database-backend', choices=('leveldb', 'unqlite'), default='unqlite',
        help='KV database to use to store CUIs and semantic types'
    )
    ap.add_argument(
        '-E', '--language', default='ENG', choices=LANGUAGES,
        help='Extract concepts of the specified language'
    )
    ap.add_argument(
        '-S', '--semantic', default='UNKNOWN',
        help='Semantic type to use if cannot be determined.'
    )
    opts = ap.parse_args()
    return opts


def main():
    opts = parse_args()

    install_spacy(opts.language)

    if not os.path.exists(opts.destination_path):
        msg = ('Directory "{}" does not exists; should I create it? [y/N] '
               ''.format(opts.destination_path))
        create = input(msg).lower().strip() == 'y'

        if create:
            os.makedirs(opts.destination_path)
        else:
            print('Aborting.')
            exit(1)

    if len(os.listdir(opts.destination_path)) > 0:
        msg = ('Directory "{}" is not empty; should I empty it? [y/N] '
               ''.format(opts.destination_path))
        empty = input(msg).lower().strip() == 'y'
        if empty:
            shutil.rmtree(opts.destination_path)
            os.mkdir(opts.destination_path)
        else:
            print('Aborting.')
            exit(1)

    if opts.normalize_unicode:
        try:
            unidecode
        except NameError:
            err = ('`unidecode` is needed for unicode normalization'
                   'please install it via the `[sudo] pip install '
                   'unidecode` command.')
            print(err, file=sys.stderr)
            exit(1)

        flag_fp = os.path.join(opts.destination_path, 'normalize-unicode.flag')
        open(flag_fp, 'w').close()

    if opts.lowercase:
        flag_fp = os.path.join(opts.destination_path, 'lowercase.flag')
        open(flag_fp, 'w').close()

    flag_fp = os.path.join(opts.destination_path, 'language.flag')
    with open(flag_fp, 'w') as f:
        f.write(opts.language)

    flag_fp = os.path.join(opts.destination_path, 'database_backend.flag')
    with open(flag_fp, 'w') as f:
        f.write(opts.database_backend)

    fhir_iterator = extract_from_fhir(opts)

    simstring_dir = os.path.join(opts.destination_path, 'umls-simstring.db')
    cuisty_dir = os.path.join(opts.destination_path, 'cui-semtypes.db')

    parse_and_encode_ngrams(fhir_iterator, simstring_dir, cuisty_dir,
                            database_backend=opts.database_backend)


if __name__ == '__main__':
    main()
