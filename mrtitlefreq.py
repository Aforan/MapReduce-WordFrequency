#!/usr/bin/env python
import mincemeat
import glob
import sys

#	Get all the filenames for the input files
files = glob.glob("hw3data/*")

#	The input data is a dictionary of filenames to file contents
data = dict((fn, open(fn, 'r').read())
	for fn in files)

#	Map function, takes a file and creates a dictionary of authors to wordlists
def mapfn(k, v):
	#	Need to declare these here, this function is called from within mincemeat and does not have access to enclosing environment
	punctuation = ['.', ',', '!', '?', '$', '*']
	allStopWords={'about':1, 'above':1, 'after':1, 'again':1, 'against':1, 'all':1, 'am':1, 'an':1, 'and':1, 'any':1, 'are':1, 'arent':1, 'as':1, 'at':1, 'be':1, 'because':1, 'been':1, 'before':1, 'being':1, 'below':1, 'between':1, 'both':1, 'but':1, 'by':1, 'cant':1, 'cannot':1, 'could':1, 'couldnt':1, 'did':1, 'didnt':1, 'do':1, 'does':1, 'doesnt':1, 'doing':1, 'dont':1, 'down':1, 'during':1, 'each':1, 'few':1, 'for':1, 'from':1, 'further':1, 'had':1, 'hadnt':1, 'has':1, 'hasnt':1, 'have':1, 'havent':1, 'having':1, 'he':1, 'hed':1, 'hell':1, 'hes':1, 'her':1, 'here':1, 'heres':1, 'hers':1, 'herself':1, 'him':1, 'himself':1, 'his':1, 'how':1, 'hows':1, 'i':1, 'id':1, 'ill':1, 'im':1, 'ive':1, 'if':1, 'in':1, 'into':1, 'is':1, 'isnt':1, 'it':1, 'its':1, 'its':1, 'itself':1, 'lets':1, 'me':1, 'more':1, 'most':1, 'mustnt':1, 'my':1, 'myself':1, 'no':1, 'nor':1, 'not':1, 'of':1, 'off':1, 'on':1, 'once':1, 'only':1, 'or':1, 'other':1, 'ought':1, 'our':1, 'ours ':1, 'ourselves':1, 'out':1, 'over':1, 'own':1, 'same':1, 'shant':1, 'she':1, 'shed':1, 'shell':1, 'shes':1, 'should':1, 'shouldnt':1, 'so':1, 'some':1, 'such':1, 'than':1, 'that':1, 'thats':1, 'the':1, 'their':1, 'theirs':1, 'them':1, 'themselves':1, 'then':1, 'there':1, 'theres':1, 'these':1, 'they':1, 'theyd':1, 'theyll':1, 'theyre':1, 'theyve':1, 'this':1, 'those':1, 'through':1, 'to':1, 'too':1, 'under':1, 'until':1, 'up':1, 'very':1, 'was':1, 'wasnt':1, 'we':1, 'wed':1, 'well':1, 'were':1, 'weve':1, 'were':1, 'werent':1, 'what':1, 'whats':1, 'when':1, 'whens':1, 'where':1, 'wheres':1, 'which':1, 'while':1, 'who':1, 'whos':1, 'whom':1, 'why':1, 'whys':1, 'with':1, 'wont':1, 'would':1, 'wouldnt':1, 'you':1, 'youd':1, 'youll':1, 'youre':1, 'youve':1, 'your':1, 'yours':1, 'yourself':1, 'yourselves':1}

	#	Function to filter puncturation and words we don't want to include in analysis
	def filt(string):
		ret = string

		#	Filter all punctuation from string
		for p in punctuation:
			ret = ret.replace(p, '')

		#	Replace hyphens with spaces
		ret = ret.replace('-', ' ')
		oldret = ret
		ret = ""

		#	Filter all stop words from string
		for word in oldret.split():
			if (not (word in allStopWords)) or len (word) <= 1:
				ret += word.lower() +  " "

		return ret

	w = {}

	#	parse line by line
	for line in v.split('\n'):
		d = line.split(":::")
		if len(d) <= 1: continue

		#get the paper id, the title of the paper, and it's authors
		pid = d[0]
		title = filt(d[2])
		authors = d[1].split('::')

		# add the contents of the title to every authors content
		for author in authors:
			if author in w:
				w[author] += title + " "
			else:
				w[author] = title + " "

	#	Yield a tuple for each key and wordlist
	for k in w.keys():
		yield k, w[k].split()

#	Reduce function, takes the key and the data for the key and sums up all the frequencies of all the wordlists in the data
def reducefn(k, vs):
	#	Sum all the words from a given list and store results in the given dictionary
	def sumWords(words, w):
		for word in words:
			if word in w:
				w[word] += 1
			else:
				w[word] = 1

		return w

	#	r keeps track of all the word frequencies
	r = {}

	#	For each wordlist, sum the word frequencies
	for v in vs:
		r = sumWords(v, r)

	#	Return a tuple of author to word frequencies
	return k, r

#	Set up mincemeat, define the map and reduce fucntions as well as the password needed to access the server
s = mincemeat.Server()
s.datasource = data
s.mapfn = mapfn
s.reducefn = reducefn

#	Run the server and get results
results = s.run_server(password="changeme")

#	Print out the results
for r in results:
	print results[r]