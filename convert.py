#!coding:utf-8
from neo4j.v1 import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "shacha"))
session = driver.session()
result = session.run("MATCH (n:Page) RETURN n Limit 100")

originNodeList = list()
for record in result:
    originNodeList.append(record[0]["title"])

f = open('./test.txt', 'w')
for node in originNodeList:
    result = session.run("Match (n:Page)-[: Link]->(end:Page) where n.title={name}  return end",{"name":node})
    string = node+"++"
    for i in result:
        string += i[0]["title"]+"--"
    string = string[:-2]
    f.write(string.encode("utf-8")+"\n")

f.close()
session.close()
