##############################
Data manipulation using Dask
##############################
import dask.bag as db

counry_bag = db.from_url('http://api.worldbank.org/countries/IND/indicators/NY.GDP.MKTP.CD?per_page=5000&format=json')
country_bag.take(1)
import json 
json_bag = country_bag.map(json.loads)
json_bag.take(1)

country_bag.take(1)
	json_format = json.loads(json_str)
	return json_format[1]
len(json_bag.compute())
url_prefix = 'http://api.worldbank.org/countries/'
url_suffix = '/indicators/NY.GDP.MKTP.CD?per_page=5000&format=json'

country_codes = ['IN','US','GBR','CN','DK',
				 'SE','SG','CA','RU','FR',
				'JP','DE','IT']
				
url_lists=[]
for code in country_codes:
	full_url = url_prefix + code + url_suffix
	url_lists.append(full_url)

print(url_lists)
				
				
				
url_lists=[]
for code in country_codes:
	full_url = url_prefix + code + url_suffix
	url_lists.append(full_url)
print(url_lists)
all_countries = db.from_url(url_lists)
all_countries.npartitions
json_countries = all_countries.map(json.loads)
json_countries.take(2,npartitions=2)
proper_json.take(10)

def corrent_json(json_str):
	json_format = json.loads(json_str)
	return json_format[1]
proper_json = all_countries.map(correct_json).flatten()

proper_json.take(3)
country_names = proper_json.pluck('country', default=None)
country_names.take(4)
country_names = proper_json.pluck('country', default=None).pluck('value')




country_names.distinct().compute()
country_list = country_names.distinct()
proper_json.npartitions


def combine_country(x):
	total =0 
	for yearly_gdp in x:
		if yearly_gdp['value'] is not None:
			total += float(yearly_gdp['value'])
	return total

test = proper_json.map_partitions(combine_country).compute()
test


def imp_columns(my_item):
	if my_item['value'] is None:
		return None
	else:
		result_dict = {
			'country': my_item['country']['value'],
			'gdp': float(my_item['value']),
			'year': int(my_item['date'])
		}
		return result_dict
filtered_bag = proper_json.map(imp_columns)

filtered_bag.take(10)
filtered_bag = filtered_bag.filter(lambda x: x is not None)
filtered_bag.take(10)
gdp_data=filtered_bag.to_dataframe().compute()

import seaborn as sns 

import matplotlib.pyplot as plt

plt.subplots(figsoze = (15,5))
sns.lineplot(data=gdp_data[gdp_data['year']>2010], x='year',y='gdp',
							hue='country')
plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

########################################
Dask scheduler
########################################
from dask  import delayed, compute 
import dask

x=list(range(2, 20000, 2))
y=list(range(3, 30000, 3))
z=list(range(5, 50000, 5)) 

final_result = [] 

def do_something_l(x, y):
	return x+y+2*x*y 

def do_somehting_2(a, b):
	return a**3 - b**3

def do_somehting_3(p,q):
	return p*p + q*q

final_result=[]
for i in range(0, len(x)):
	res_1=delayed(do_something_1)(x[i], y[i])
	res_2=delayed(do_something_2)(y[i], z[i])
	res_3=delayed(do_something_3)(res_1, res_2)
	final_result.append(res_3)
final_sum=delayed(sum)(final_result)

with dask.config.set(scheduler='threading'):
	%time _ = final_sum.compute()

with dask.config.set(scheduler='processes'):
	%time _ = final_sum.compute()

with dask.config.set(scheduler='sync'):
	%time _ = final_sum.compute()
	
x=[2,4,6,8]
y=[3,6,9,12]
z=[5,10,15,20]
final_result=[]

import time 

def do_somehting_1(x,y):
	time.sleep(10)
	return x + y + 2*x*y

def do_somehting_2(a,b):
	time.sleep(10)
	return a**3 - b**3

def do_somehting_3(p,q):
	time.sleep(10)
	return p*p + q*q

final_result = [] 
for i in range(0, len(x)):
	res_1 = delayed(do_somehting_1)(x[i],y[i])
	res_2 = delayed(do_somehting_2)(y[i],z[i])
	res_3 = delayed(do_somehting_3)(res_1,res_2)
	final_result.append(res_3)

final_sum = delayed(sum)(final_result)

with dask.config.set(scheduler='processes'):
	%time _ = final_sum.compute()

with dask.config.set(scheduler='sync'):
	%time _ = final_sum.compute()

with dask.config.set(scheduler='threading'):
	%time _ = final_sum.compute()
	

from dask.distributed import Client
import dask.dataframe as df

client = Client(processes = False, thread_per_worker=2,
				n_workers=3, memory_limit='4GB')
client

dummy_df = df.read_csv('broken_csvs/library-part-0[0-2]*.csv')
dummy_df 
dummy_df.describe().compute()
dummy_df.head()
dummy_df = dummy_df.drop('Unname: 0',axis=1)
group_publication_yr = dummy_df.groupdy('PublicationYear').count()
group_publication_yr.compute()
max_gdp_per_country = dummy_df.groupby('Author')['ItemCount'].sum()
max_gdp_per_country.compute()
len(dummy_df)

import tr
def extract_year(year_text, *args, **kwargs):
	if type(year_text)==type(''):
		return re.findall("[-+]?[\d]+(?:,\d\d\d)*[\.]?\d*(?:[eE[-+]?\d+)?", year_text)
	else:
		return year_text

separate_republication_year = dummy_df['PublicationYear'].apply(extract_year,
															axis=1,
															meta=('PublicationYear','object'))
seperate_publication_year.compute()
dummy_df.nlargest(5, 'ItemCount').compute()

from dask.distributed import Client
import dask.dataframe as df 

client = Client(processes=False, thread_per_worker=2,
					n_workers=3, memory_limit='4GB')
client

import dask.dataframe as df 
dummy_df = df.read_csv('/multiple_csvs/*.csv')
dummy_df 
dummy_df.describe().visualize(filename='describe.png')
dummy_df.head()
dummy_df = dummy_df.drop('Unnamed: 0', axis=1)
dummy_df.head() 
max_gdp_per_country = dummy_df.groupby('country')['gdp'].max()
max_gdp_per_country.compute()
without_japan = dummy_df[dummy_df.country != 'Japan']
without_japan.visualize()
persisted_df = without_japan.persist()
persisted_gdp = persisted_df.groupby('country')['gdp'].max()
persisted_df.visualize()
persisted_df = without_japan.persist()



from dask.distributed import Client 
import dask.dataframe as df

client = Client(processes=False, threads_per_worker=2,
				n_workers=3, memory_limit='4GB')

import time

def some_func(x):
	time.sleep(1)
	y = x + 1
	time.sleep(1)
	z = x * y
	time.sleep(1)
	return z

result_1 = client.submit(some_func, 5)
result_l

def some_func_long(x):
	time.sleep(10)
	y = x + 1
	time.sleep(5)
	z = x * y 
	time.sleep(2)
	return z

result_2 = client.submit(some_func_long, 5)
result_2 

/*** result() function can be used to force the computation to complete ***/
result_1.result()
result_3 = client.submit(some_func_long, 15)
result_3.result()

/*** We can evaluate multiple Future objects in one go using gather ***/

futures_list = [client.submit(some_func, x) for x in range(1, 5)]
all_res = client.gather(futures_list)
all_res

/***  use map function to pass multiple future object ***/

list_result = client.map(some_func_long, range(5))
client.gather(list_result)

from sklearn.datasets import make_regression 
from sklearn.linear_model import LinearRegression, Ridge 

X, y = make_regression(n_sample=10000,
						random_state=0,
						n_features=10,
						n_informative=6,
						)
print(len(X[0])
print(X)
clf = LinearRegression()
clf.fit(X,y)
clf2=Ridge()
clf2.fit(X,y)

from sklearn.externals import joblib 

with joblib.parallel_backend('dask'):
	clf.fit(X,y)

clf.predict(X)[:5]

from dask_ml.cluster import KMeans
import dask_ml.datasets 

X,y = dask_ml.datasets.make_blobs(n_samples=500000,
								  chunks=50000,
								  random_state=0
								  centers=5)

clf = KMeans(n_clusters=5, init_max_iter=10)
clf.fit(X)
my_pred=clf.predict(X[:10])
my_pred.compute()

from sklearn.svm import SVC 
from sklearn.datasets import make_classification
from sklearn.model_selection import GridSearchCV 

X,y = make_classification(n_samples=500,
						  random_state=0,
						  n_classes=3,
						  n_features=5,
						  n_informative=3,
						  n_redundant=2)
len(X[0])

param_grid= {
	"C": [0.00001, 0.0001, 0.001, 0.01, 0.1, 1],
	"kernel": ['rbf', 'poly', 'sigmoid'],
	"degree": [1, 2, 3, 4],
	"coef0": [1, 0.5, 0.3, 0.2, 0.1],
	"gamma": ["auto", "scale"]
			}

clf = SVC(random_state=0, probability=True)

grid_search = GridSearchCV(clf,
							param_grid=param_grid,
							cv=3,
							n_jobs=-1)
%time grid_search.fit(X,y)

from sklearn.externals import joblib 

with joblib.parallel_backend('dask'):
	%time _ = grid_search.fit(X,y)

grid_search.predict(X)[:10]
