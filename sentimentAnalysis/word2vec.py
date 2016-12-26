#python example to train doc2vec model (with or without pre-trained word embeddings)
from gensim import utils
from gensim.models.doc2vec import TaggedDocument
from gensim.models import Doc2Vec
from random import shuffle
import sys
import logging

#doc2vec parameters
vector_size = 300
window_size = 8
min_count = 1
sampling_threshold = 1e-5
negative_size = 5
train_epoch = 20
dm = 0 #0 = dbow; 1 = dmpv
worker_count = 2 #number of parallel processes

#input corpus
train_corpus = "../../backupData/RedittNews8Years/RedditCleanedNewsForVectors.txt"
train_corpus2 = "../../backupData/crawledProcessedData/newsFeedsForVectorFormation.txt"

#output model
saved_path = "../../backupData/models/RedditModel.bin"


class TaggedLineSentence(object):
	def __init__(self, sources):
		self.sources = sources

		flipped = {}

		# make sure that keys are unique
		for key, value in sources.items():
			if value not in flipped:
				flipped[value] = [key]
			else:
				raise Exception('Non-unique prefix encountered')

	def __iter__(self):
		for source, prefix in self.sources.items():
			with utils.smart_open(source) as fin:
				for item_no, line in enumerate(fin):
					yield TaggedDocument(utils.to_unicode(line).split(), [prefix + '_%s' % item_no])

	def to_array(self):
		self.sentences = []
		for source, prefix in self.sources.items():
			with utils.smart_open(source) as fin:
				for item_no, line in enumerate(fin):
					self.sentences.append(TaggedDocument(utils.to_unicode(line).split(), [prefix + '_%s' % item_no]))
		return self.sentences

	def sentences_perm(self):
		shuffle(self.sentences)
		return self.sentences

sources = {train_corpus:'TRAIN',train_corpus2:'TRAIN_OR'}
sentences = TaggedLineSentence(sources)

#enable logging
log = logging.getLogger()
log.setLevel(logging.DEBUG)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
log.addHandler(ch)

#train doc2vec model
model = Doc2Vec(min_count=1, window=10, size=300, sample=1e-4, negative=5, workers=7)
model.build_vocab(sentences.to_array())

log.info('Epoch')
for epoch in range(10):
	log.info('EPOCH: {}'.format(epoch))
	model.train(sentences.sentences_perm())

#save model
model.save(saved_path)