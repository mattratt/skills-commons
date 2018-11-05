import sys
import json
import numpy as np
from sklearn.decomposition import LatentDirichletAllocation
from sklearn.feature_extraction.text import CountVectorizer
import nltk
from nltk.corpus import stopwords
import skills_commons as sc
import skills_commons_db as scdb


log = sc.get_log(__name__)


def dump_topic_word_distribs(lda_model, word_vectorizer, outfile_name, threshold=1.1):

    topic_distribs = lda_model.components_ / lda_model.components_.sum(axis=1)[:, np.newaxis]
    num_topics, num_words = topic_distribs.shape

    word_names = word_vectorizer.get_feature_names()

    with open(outfile_name, 'w') as outfile:
        for topic_id in range(num_topics):
            topic_distrib = topic_distribs[topic_id, :]
            word_freqs = [ (f, word_names[w]) for w, f in enumerate(topic_distrib) ]
            word_freqs.sort(reverse=True)

            topic_words = []
            mass = 0.0
            for freq, word in word_freqs:
                topic_words.append((word, freq))
                mass += freq
                if mass > threshold:
                    break
            json_str = json.dumps([topic_id, topic_words])
            outfile.write(json_str + "\n")

            try:
                topic_words_str = "\n\t".join(["{}\t{:0.4f}".format(w, f) for w, f in topic_words])
                print "topic {}:\n\t{}\n".format(topic_id, topic_words_str)
            except UnicodeEncodeError as e:
                print "topic {}: ERR".format(topic_id)


def dump_course_topic_distribs(course_ids, file_names, lda_distribs, outfile_name):
    with open(outfile_name, 'w') as outfile:
        for course_id, file_name, topic_distrib in zip(course_ids, file_names, lda_distribs):
            json_str = json.dumps([course_id, file_name, topic_distrib.tolist()])
            outfile.write(json_str + "\n")


def print_topic_word_distribs_from_file(filepath):
    with open(filepath, 'r') as infile:
        for line in infile:
            topic_id, word_freqs = json.loads(line.rstrip("\n"))
            print "topic {} word_freqs length {}".format(topic_id, len(word_freqs))
            for word, freq in word_freqs[:10]:
                print "\t", word, freq
            print ''


def clean_text(text_dirty, stops, bigrams=None):
    tokens = [ w.lower() for w in nltk.wordpunct_tokenize(text_dirty) ]
    # tokens = replace_bigrams(tokens, bigrams)
    words = [ w for w in tokens if w.isalpha() and (w not in stops) ]
    words_str = " ".join(words)
    return words_str


###################################
if __name__ == '__main__':

    host = sys.argv[1]
    dbname = sys.argv[2]
    user = sys.argv[3]
    n_topics = int(sys.argv[4])

    conn = scdb.get_connection(host, dbname, user)

    log.info("getting file texts")
    course_text_tups = scdb.get_course_text(conn)
    stops = set(stopwords.words('english'))
    log.debug("got {} stop words: {}...".format(len(stops), list(stops)[:15]))
    course_ids = []
    file_names = []
    texts = []
    for i, (course_id, file_name, text) in enumerate(course_text_tups):
        if i % 10000 == 0:
            log.debug("\t{}".format(i))
        course_ids.append(course_id)
        file_names.append(file_name)
        texts.append(clean_text(text, stops))

    # course_ids = [ tup[0] for tup in course_text_tups ]
    log.debug("got {} course ids: {}...".format(len(course_ids), course_ids[:5]))
    # file_names = [ tup[1] for tup in course_text_tups ]
    log.debug("got {} file names: {}...".format(len(course_ids), file_names[:5]))
    # texts = [clean_text(tup[2], stops) for tup in course_text_tups]

    #zzz trying to figure out source of multiarray error
    texts = [t[:2000] for t in texts]
    log.debug("got {} texts from db: {}\n\n{}\n\n{}".format(len(texts),
                                                            texts[0], texts[1], texts[2]))
    log.info("vectorizing")
    termfreq_vectorizer = CountVectorizer()
    texts_vectored = termfreq_vectorizer.fit_transform(texts)

    log.info("learning lda model")
    NORMALIZED = True
    lda_model = LatentDirichletAllocation(n_topics=n_topics,
                                          learning_method='batch',
                                          evaluate_every=10,
                                          n_jobs=2,
                                          verbose=10,
                                          doc_topic_prior=None,
                                          topic_word_prior=None)
    if NORMALIZED:
        log.debug("fitting normalized")
        content_lda = lda_model.fit_transform(texts_vectored)
    else:
        log.debug("fitting ")
        lda_model.fit(texts_vectored)
        content_lda, _ = lda_model._e_step(texts_vectored, cal_sstats=False, random_init=False)
    log.debug("components_ shape: {}".format(lda_model.components_.shape))
    log.debug("content_lda shape: {}".format(content_lda.shape))

    dump_course_topic_distribs(course_ids, file_names, content_lda,
                               'lda_course_topic_distribs_{}.tsv'.format(n_topics))

    dump_topic_word_distribs(lda_model, termfreq_vectorizer,
                             'lda_topic_word_distribs_{}.tsv'.format(n_topics),
                             threshold=0.25)


