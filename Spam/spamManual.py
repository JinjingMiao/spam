
import numpy
import collections
from sklearn import tree
from elasticsearch import Elasticsearch
import csv




es = Elasticsearch(timeout=1500)
spamWords = []
spamDict = {}
indexName = 'spam_dataset_test'
indexSize = 7500
featureDict = dict()


def generateSpamDict(dataset):

	for word in spamWords:
		body = {"query": { "bool": { "must": {"query_string": {"default_field": "split","query": dataset}},"filter": { "term":  { "body": "word" }}}},"size": 6335}
		res = es.search(index=indexName, body=body)
		ids = [d['_id'] for d in res['hits']['hits']]
		spamDict[word] = ids
	return spamDict


def createMatrix(dataset):
	generateSpamDict(dataset)
	if dataset == 'training':
		trainDocList = dict()
		with open('./trainingMatrix.txt', 'w') as trainMatrix:
			res = es.search(index=indexName, 
                            body={
                                'query': {
                                    'query_string': {
                                        "default_field": "split",
                                        "query": "training"
                                    }
                                },
                                "size": indexSize
                            })
			ids = [d['_id'] for d in res['hits']['hits']]
			count = 0
			for id in ids:
				count += 1
				trainDocList[id] = count
				tempList = list()
				for features in spamWords:
					try:
						if id in spamDict[features]:
							tempList.append("1")
						else:
							tempList.append("0")
					except KeyError:
						tempList.append("0")
				out = ' '.join(tempList)
				lab = es.get(index=indexName, id=id)['_source']['label']
				if lab == 'spam':
					label = '1'
				else:
					label = '0'
				trainMatrix.write(label + " " + out + '\n')
		with open('./train_ids_list.txt', 'w') as trains:
			for k in sorted(trainDocList, key=trainDocList.get):
				trains.write(str(k)+" "+str(trainDocList[k])+'\n')
	else:
		testDocList = dict()
		with open('./testMatrix.txt', 'w') as testMatrix:
			res = es.search(index=indexName,
                            body={
                                'query': {
                                    'query_string': {
                                        "default_field": "split",
                                        "query": "test"
                                    }
                                },
                                "size": indexSize
                            })
			ids = [d['_id'] for d in res['hits']['hits']]
			count = 0
			for id in ids:
				print ("Spam Matrix done for " + id)
				count += 1
				testDocList[id] = count
				tempList = list()
				for features in spamWords:
					try:
						if id in spamDict[features]:
							tempList.append("1")
						else:
							tempList.append("0")
					except KeyError:
						tempList.append("0")
				out = ' '.join(tempList)
				lab = es.get(index=indexName, id=id)['_source']['label']
				if lab == 'spam':
					label = '1'
				else:
					label = '0'
				testMatrix.write(label + " " + out + '\n')
		with open('./test_ids_list.txt', 'w') as tests:
			for k in sorted(testDocList, key=testDocList.get):
				tests.write(str(k)+" "+str(testDocList[k])+'\n')


if __name__ == "__main__":
	with open('./spam_words.txt', 'r') as f:
		for line in f.readlines():
			word = line.replace('\n', '')
			spamWords.append(word)
	i = 0
	for word in spamWords:
		i += 1
		featureDict[word] = i
	createMatrix("training")
	createMatrix("test")
	#modelData()
	getOverallAccuracy()
