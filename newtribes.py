from neo4j import GraphDatabase
import pandas as pd
import json
import codecs

# Database Credentials

uri             = "bolt://localhost:7687"

userName        = "neo4j"

password        = "1234"

 

# Connect to the neo4j database server

graphDB_Driver  = GraphDatabase.driver(uri, auth=(userName, password))

 

# CQL to query all the teams present in the graph

cqlNodeQuery          = "MATCH (x:Team) RETURN x"

 

# CQL to query for teams to temas relationship

cqlEdgeQuery          = "MATCH (x:Team )-[r]->(y:Team) RETURN y.name,x.name"


## Read Schema into a pandas dataframe
schema= pd.read_csv("GraphSchema.csv")

## create a dictionary from json file
json_file=json.load(codecs.open('tribesai.json', 'r', 'utf-8-sig'))
z=[]
# recursive function like dfs to return required strings from the dictionary
def f(d,i,s):
  #print(d)
  if i==len(dummylist):
    z.append(d)
    return
  else:
    if (dummylist[i]!="0-N"):
      f(d[dummylist[i]],i+1,s)
    else:
      for j in d:
        f(j,i+1,s)
schkeys=list(schema.keys())
ls=len(schema[schkeys[0]])
q=[]
di=dict()
print(ls)  
query="CREATE"
for i in range(ls):
  if schema["Node/Relationship"][i]=="Node":
    i1=schema["IdObject"][i]
    dummylist=list(i1.split("►"))
    d=json_file[dummylist[0]]
    z=[]
    f(d,1,"")
    
    
    z1=z[:]
    
    if i!=6:
      n1=schema["Name"][i]
      dummylist1=list(n1.split("►"))
      d=json_file[dummylist1[0]]
      z=[]
      f(d,1,"")
    #replace - with _ to be used in query
    for j in range(len(z)):
      z1[j]=z1[j].replace("-","_")
      z[j]=z[j].replace("-","_")
      z1[j]=z1[j].replace(" ","_")
      z[j]=z[j].replace(" ","_")
      u="("+z[j]+":"+schema['Label (Primary)'][i]+" { name : \""+z1[j]+"\"}),\n"
      query+=u
    di[schema['Label (Primary)'][i]]=z1[:]
  else:
    rel_n=schema["RelationshipName"][i]
    a,b=schema["Pair"][i].split("->")
    for j in di[a]:
      for k in di[b]:
       u="("+j+")-[:"+rel_n+"]->("+k+"),\n"
       query+=u
query=list(query)
##remove the last comma from the query
query.pop()
query.pop()
query="".join(query)
dummylist=list(query.split(","))
dummylist1=[]
s=set()
for i in dummylist:
  if i not in s:
    s.add(i)
    dummylist1.append(i)
# CQL to create a graph 
print("the queries")
print(",".join(dummylist1))
cqlCreate=""+",".join(dummylist1)+""

print(cqlCreate)
# Execute the CQL query
with graphDB_Driver.session() as graphDB_Session:
    # Create nodes
    graphDB_Session.run(cqlCreate)
    # Query the graph    
    nodes = graphDB_Session.run(cqlNodeQuery)
    print("List of Teams universities present in the graph:")
    for node in nodes:
        print(node)
    # Query the relationships present in the graph
    nodes = graphDB_Session.run(cqlEdgeQuery)
    print("Team-Team relationships present in the Graph:")
    for node in nodes:
        print(node)