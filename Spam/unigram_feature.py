__author__ = 'yixing'

from elasticsearch import Elasticsearch
import string
import pickle
import operator
from liblinear.python.liblinearutil import *

LABEL_PATH = '/Users/yixing/Documents/trec07p/full/index'
RESULT_PATH = '/Users/yixing/Documents/CS6200/Homework7/result'
INDEX = 'spam'
DOC_TYPE = 'document'

label_map = {}
dictionary = {}
index = 0
train_set = []
test_set = []
train_index = []
test_index = []

def read_label_to_map():
    fh = open(LABEL_PATH, "r")
    for line in fh.readlines():
        new_line = string.strip(line, '\n')
        # print new_line
        [label, path] = new_line.split(' ')
        doc_id = path.split('/')[-1]
        # print doc_id, label
        if label == 'spam':
            grade = 1
        else:
            grade = 0
        label_map[doc_id] = grade

def read_train_test_set():
    train_path = RESULT_PATH + '/' + 'train_set'
    test_path = RESULT_PATH + '/' + 'test_set'
    train_fh = open(train_path, "r")
    for line in train_fh.readlines():
        doc_id = string.strip(line, '\n')
        train_set.append(doc_id)
    train_fh.close()
    test_fh = open(test_path, "r")
    for line in test_fh.readlines():
        doc_id = string.strip(line, '\n')
        test_set.append(doc_id)
    test_fh.close()

def query_doc(es,doc_id):
    global index
    resp = es.termvector(index=INDEX,doc_type=DOC_TYPE, id=doc_id)
    term_map = {}
    try:
        terms = resp['term_vectors']['text']['terms']
    # print "The length of", doc_id, "is", len(terms)
    except:
        return term_map
    for word in terms:
        tf = terms[word]['term_freq']
        # print word, tf
        if word not in dictionary:
            index += 1
            dictionary[word] = index
            term_map[index] = tf
        else:
            word_index = dictionary[word]
            term_map[word_index] = tf
    print "The length of this doc term_map is:", len(term_map)
    return term_map

def write_feature_matrix(es):
    train_matrix_path = RESULT_PATH + '/' + 'train_feature_matrix'
    test_matrix_path = RESULT_PATH + '/' + 'test_feature_matrix'
    train_fh = open(train_matrix_path, "w")
    test_fh = open(test_matrix_path, "w")
    count = 0
    # train_size = 0
    # test_size = 0
    for doc_id in sorted(label_map.keys()):
        print doc_id
        feature_map = query_doc(es, doc_id)
        label = label_map[doc_id]
        if doc_id in train_set:
            # train_size += 1
            write_vector_to_file(label, feature_map, train_fh)
            train_index.append(doc_id)
        elif doc_id in test_set:
            # test_size += 1
            write_vector_to_file(label, feature_map, test_fh)
            test_index.append(doc_id)
        count += 1
    train_fh.close()
    test_fh.close()
    write_index_file('train_index',train_index)
    write_index_file('test_index',test_index)
    print "The train set length is:", len(train_index)
    print "The test set length is:", len(test_index)
    print "The total size is:", count

def write_vector_to_file(label, feature_matrix, fh):
    fh.write('%d'%label)
    if feature_matrix:
        for index in sorted(feature_matrix.keys()):
            fh.write(' %d:%d'%(index, feature_matrix[index]))
    fh.write('\n')

def write_index_file(filename,index):
    outpath = RESULT_PATH + '/' + filename
    fh = open(outpath, "w")
    for doc_id in index:
        fh.write('%s\n'%doc_id)
    fh.close()

def write_dictionary_file():
    # outpath = RESULT_PATH + '/' + 'dictionary'
    # fh = open(outpath, "w")
    # sorted_dictionary = sorted(dictionary.items(), key=operator.itemgetter(1))
    # for pair in sorted_dictionary:
    #     fh.write('%s\t%d'%(pair[0], pair[1]))
    outpath = RESULT_PATH + '/' + 'dictionary.p'
    pickle.dump(dictionary, open(outpath,"wb"))


def main():
    es = Elasticsearch()
    # query_doc(es,'inmail.1')
    read_label_to_map()
    read_train_test_set()
    write_feature_matrix(es)
    write_dictionary_file()

if __name__ == '__main__':
    main()