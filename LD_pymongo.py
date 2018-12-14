#!/usr/bin/env python

from pymongo import MongoClient
import pymongo, glob, os, sys
from collections import defaultdict


class MongoDB_setting():
	def __init__(self, Species, Making_Index):
		client = MongoClient('localhost', 27017)
		Target_LD = Species + "_" + "LD"
		self.db = client[Target_LD]
		self.collections_lists = self.db.collection_names()

		if Making_Index == True: #Making Index takes long times......
			for chrom in self.collections_lists:
				print("{0}: Creating Indexing".format(chrom))
				collection = self.db[chrom]
				collection.create_index([
										 ("dprime",pymongo.ASCENDING),
										 ("qname",pymongo.TEXT),
										 ("tname",pymongo.TEXT)
										 ],
										 partialFilterExpression={"dprime":{"$gt":0.8}},
										 default_language='english',
										 background=True, name='LD_index')

class Mongo_Indexing(MongoDB_setting):
	def file_loading(self, RS_lists_folder):
		self.Disease_Snps = defaultdict(list)
		Disease_lists = glob.glob(RS_lists_folder)
		for dis in Disease_lists:
			dis_name = dis.rsplit("/", 1)[1]
			with open(dis, "r") as f:
				for chr_rs_id in f.readlines():
					chr_rs_id = chr_rs_id.rstrip()
					self.Disease_Snps[dis_name].append(chr_rs_id)

	def Indexing_output(self, RS_lists_output):
		print(len(self.collections_lists), self.collections_lists)
		if(not(os.path.exists(RS_lists_output))):	os.makedirs(RS_lists_output)

		for dis_name in self.Disease_Snps:
			temp_tab_list = self.Disease_Snps[dis_name]
			chrom_list = ['chr'+ t_i.split('\t')[0] for t_i in temp_tab_list]
			rs_list = [t_i.split('\t')[1] for t_i in temp_tab_list]
			chrom_list = list(set(chrom_list)) ; rs_list = list(set(rs_list))
			print('\n--------------------------------------------')
			print(dis_name)
			print(rs_list)
			LD_dis = "LD_" + dis_name
			NO_LD_dis = "NO_LD_" + dis_name
			RS_LD_ouput = os.path.join(RS_lists_output, LD_dis)
			RS_NO_LD_ouput = os.path.join(RS_lists_output, NO_LD_dis)

			if len(rs_list) < 3:
				rs_write = '\n'.join(rs_list)
				with open(RS_NO_LD_ouput, "w") as no_w:
					no_w.write(rs_write)
				continue

			After_LD_list = []
			for chrom in self.collections_lists:
				if chrom in chrom_list:
					collection = self.db[chrom]
					Index_out = collection.find(
											#Query
											{ "$and":
													[
														{"dprime":{"$gt":0.8}},
										    			{"qname":{"$in":rs_list}},
										    			{"tname":{"$in":rs_list}}
										    		]
										  	},
										  	#Projection
										  	# {"qname":1,"tname":1,"corr":1,"dprime":1}
										  	{"qname":1,"tname":1,"dprime":1}
										  )
					#Writing
					myValues=[]
					for i in Index_out:
						myValues.append(str(i['qname']))
						myValues.append(str(i['tname']))

					Result_rs_list = list(set(myValues))
					print(chrom, Result_rs_list)
					After_LD_list.extend(Result_rs_list)

			After_LD_list = list(set(After_LD_list))
			Final_LD_rsult =  '\n'.join(After_LD_list)
			if Final_LD_rsult == '':
				print('No LD in this disease')
				Final_LD_rsult = '\n'.join(rs_list)
				with open(RS_NO_LD_ouput, "w") as no_w:
					no_w.write(Final_LD_rsult)
			else:
				print(Final_LD_rsult)
				with open(RS_LD_ouput, "w") as w:
					w.write(Final_LD_rsult)


if __name__ == "__main__":
	print('LD_pymongo.py START\n\n')
	print("###### Connecting MongoDB ######\n")

	Species = 'JPT'
	Making_Index = False
	MongoDB = MongoDB_setting(Species, Making_Index)

	RS_lists_folder = './Input/*'
	RS_lists_output = './Output'
	Mongo_index_class = Mongo_Indexing(Species, Making_Index)
	Mongo_index_class.file_loading(RS_lists_folder)
	Mongo_index_class.Indexing_output(RS_lists_output)

	print('LD_pymongo.py END\n\n')
