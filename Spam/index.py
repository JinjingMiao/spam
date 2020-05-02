import random
import re
import string
import warnings

from elasticsearch import Elasticsearch
from bs4 import BeautifulSoup
import os




es = Elasticsearch()
corpus_path = './trec07p/data'
label_path = './trec07p/full'
lable_filename = 'index'
stopwords = []
dictionary = []


def getLabels():
	labelDict = dict()
	with open(label_path+'/'+lable_filename, 'r') as f:
		for line in f:
			label_list = line.split()
			labelDict[label_list[1].split('/')[2]] = '1' if label_list[0] == 'spam' else 'ham'
	return labelDict


def computeIndex(dataSet, labelDict, setname):
	warnings.warn("deprecated", DeprecationWarning)
	for file in dataSet:
		with open(file, 'r',encoding='ISO-8859-1') as f:
			fileData = f.read()
			data = fileData.replace('\n', ' ').replace('\t', '')

			words = data.split()
			withoutStopwords= list()

			for i in range(len(words)):
				if re.match("^[A-Za-z]*$", words[i]):
					if words[i] in stopwords:
						withoutStopwords.append('')
					else:
						withoutStopwords.append(words[i])
				else:
					withoutStopwords.append('')
			text = ' '.join(withoutStopwords)
			text = re.sub(' +', ' ', text)
			text = text.lower()

			for p in string.punctuation:
				if p != '_' and p != '-' and p != '\'':
					text = text.replace(p, " ")
			text = text.replace("  ", " ")
			text = BeautifulSoup(text, "html.parser").text

			dataFile = f.name.split('/')[3]

			if labelDict[dataFile] == '1':
				spamVal = 'spam'
			else:
				spamVal = 'ham'

			bodyText = {
				'file_name': dataFile,
				'label': spamVal,
				'body': text,
				'split': setname
			}

			es.index(index='spam_dataset_test', id=dataFile, body=bodyText)


def createIndex():
	labelDict = getLabels()

	for dir_path, dir_names, file_names in os.walk(corpus_path):
		allfiles = [os.path.join(dir_path, filename).replace("\\", "/") for filename in file_names]
	training = allfiles
	test = list(random.sample(training, 15084))
	for f in test:
		training.remove(f)

	computeIndex(training, labelDict, 'training')
	computeIndex(test, labelDict, 'test')


if __name__ == "__main__":
	if es.ping():
		es.indices.delete(index="spam_dataset_test", ignore=[400, 404])
		print("Deleted index spam_dataset if it already existed.")
		with open('./stoplist.txt', 'r') as f:
			for line in f.readlines():
				word = line.replace('\n', '')
				stopwords.append(word)


		createIndex()
