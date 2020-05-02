

__author__ = 'yixing'


from liblinear.python.liblinearutil import *
import string
import pickle
import operator

RESULT_PATH = '/Users/yixing/Documents/CS6200/Homework7/result'
# from liblinearutil import *
model_path = RESULT_PATH + '/' + 'model'


dictionary = {}

def run_liblinear():
    train_path = RESULT_PATH + '/' + 'train_feature_matrix'
    test_path = RESULT_PATH + '/' + 'test_feature_matrix'
    # model_path = RESULT_PATH + '/' + 'model'
    y1, x1 = svm_read_problem(train_path)
    y2, x2 = svm_read_problem(test_path)
    m = train(y1, x1, '-c 4')
    save_model(model_path, m)
    test_res = RESULT_PATH + '/' + 'test_result'
    test_fh = open(test_res, "w")
    p_label, p_acc, p_val = predict(y2,x2,m)
    print p_acc
    # print type(p_acc)
    # test_fh.write('%s'%p_label)
    test_fh.write('%s %s %s'%(p_acc[0],p_acc[1],p_acc[2]))
    test_fh.close()
    # print p_acc
    # print p_val
    train_res = RESULT_PATH + '/' + 'train_result'
    train_fh = open(train_res, "w")
    p_label, p_acc, p_val = predict(y1,x1,m)
    print p_acc
    # train_fh.write('%s'%p_label)
    train_fh.write('%s %s %s'%(p_acc[0],p_acc[1],p_acc[2]))
    train_fh.close()
    # print p_acc
    # print p_val
def analyse_model():
    model_fh = open(model_path, "r")
    parameter_begin = False
    parameter_map = {}
    index = 0
    for line in model_fh.readlines():
        new_line = string.strip(line, "\n")
        if new_line == "w":
            parameter_begin = True
            continue
        if parameter_begin == True:
            index += 1
            unigram = dictionary[index]
            parameter_map[unigram] = float(line)
    sorted_parameter_map = sorted(parameter_map.items(), key=operator.itemgetter(1), reverse=True)
    model_fh.close()
    outpath = RESULT_PATH + '/' + 'unigram_rank'
    rank_fh = open(outpath,"w")
    for pair in sorted_parameter_map:
        rank_fh.write('%s\t%f\n'%(pair[0],pair[1]))
    rank_fh.close()



def read_dictionary():
    global dictionary
    inpath = RESULT_PATH + '/' + 'dictionary.p'
    reverse_dictionary = pickle.load(open(inpath, "rb"))
    for key in reverse_dictionary:
        dictionary[reverse_dictionary[key]] = key
    # print "The virgra index is:", reverse_dictionary['viagra']


def main():
    run_liblinear()
    read_dictionary()
    analyse_model()


if __name__ == '__main__':
    main()