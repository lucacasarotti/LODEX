import tornado.ioloop
import traceback
import tornado.web
import os
from operator import itemgetter
import igraph
from igraph import *
import pprint
import motor
import time
from tornado import gen

exclusion = []

class MainHandlerOk(tornado.web.RequestHandler):
    def get(self):
        self.set_header('Content-Type', '') # I have to set this header 
        #https://stackoverflow.com/questions/17284286/disable-template-processing-in-tornadoweb
        #https://github.com/tornadoweb/tornado/blob/master/tornado/template.py
        #http://www.tornadoweb.cn/en/documentation#templates
        #https://github.com/tornadoweb/tornado/blob/master/tornado/template.py
        #self.render('LODeX.html')
        with open('LODeX.html', 'r') as file:
            self.write(file.read())
class SchemaSummary(tornado.web.RequestHandler):
    def get(self,endpoint_id):
        self.set_header('Content-Type', '') # I have to set this header 
        #https://stackoverflow.com/questions/17284286/disable-template-processing-in-tornadoweb
        #https://github.com/tornadoweb/tornado/blob/master/tornado/template.py
        #http://www.tornadoweb.cn/en/documentation#templates
        #https://github.com/tornadoweb/tornado/blob/master/tornado/template.py
        self.render('ss.html')

class ClusterSchema(tornado.web.RequestHandler):
    def get(self,endpoint_id):
        self.set_header('Content-Type', '') # I have to set this header 
        #https://stackoverflow.com/questions/17284286/disable-template-processing-in-tornadoweb
        #https://github.com/tornadoweb/tornado/blob/master/tornado/template.py
        #http://www.tornadoweb.cn/en/documentation#templates
        #https://github.com/tornadoweb/tornado/blob/master/tornado/template.py
        self.render('cs.html')
        

class About(tornado.web.RequestHandler):
    def get(self):
        self.set_header('Content-Type', '') # I have to set this header 
        #https://stackoverflow.com/questions/17284286/disable-template-processing-in-tornadoweb
        #https://github.com/tornadoweb/tornado/blob/master/tornado/template.py
        #http://www.tornadoweb.cn/en/documentation#templates
        #https://github.com/tornadoweb/tornado/blob/master/tornado/template.py
        #self.render('LODeX.html')
        with open('about.html', 'r') as file:
            self.write(file.read())

class IndexDatasetHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @gen.coroutine
    def get(self):
        exid = [a['_id'] for a in exclusion]
        #         pprint.pprint(exid)
        db = self.settings['db']  #
        cursor = db.lodex.ike.find({'ss': {'$exists': True}, '_id': {'$nin': exid}
                                    })
        res = []
        while (yield cursor.fetch_next):
            tmp = cursor.next_object()
            res.append({'id': tmp['_id'], 'name': tmp['name'] if 'name' in tmp else None,
                        'uri': tmp['uri'], 'triples': tmp['triples'] if 'triples' in tmp else None,
                        'instances': tmp['instances'] if 'instances' in tmp else None,
                        'propCount': len(tmp['propList']) if 'propList' in tmp else None,
                        'classesCount': len(tmp['classes']) if 'classes' in tmp else None})
        self.content_type = 'application/json'
        print(len(res))
        self.write({'data': res})
        self.finish()


class IndexDatasetHandlerFull(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @gen.coroutine
    def get(self):
        exid = [a['_id'] for a in exclusion]
        #         pprint.pprint(exid)
        db = self.settings['db']  #
        cursor = db.lodex.ike.find({'ss': {'$exists': True}, '_id': {'$nin': exid}
                                    })
        res = []
        while (yield cursor.fetch_next):
            tmp = cursor.next_object()
            res.append({'id': tmp['_id'], 'name': tmp['name'] if 'name' in tmp else None,
                        'uri': tmp['uri'], 'triples': tmp['triples'] if 'triples' in tmp else None,
                        'instances': tmp['instances'] if 'instances' in tmp else None,
                        'propCount': len(tmp['propList']) if 'propList' in tmp else None,
                        'classesCount': len(tmp['classes']) if 'classes' in tmp else None,
                        'classList': tmp['classes'] if 'classes' in tmp else None,
                        'propList': tmp['propList'] if 'propList' in tmp else None
                        })
        self.content_type = 'application/json'
        self.write({'data': res})
        self.finish()


class GraphHandler(tornado.web.RequestHandler):
    def get(self, endpoint_id):
        self.render('LODeX.html', endpoint_id=endpoint_id)


class DataHandlerSS(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self, endpoint_id):
        db = self.settings['db']
        db.lodex.ike.find_one({'_id': int(endpoint_id)},
                              callback=self._on_response)

    def _on_response(self, response, error):
        ss = response['ss']
        ss.update(
            {'name': response['name'], 'id': response['_id'], 'uri': response['uri']})
        chunk = createSS(ss)
        self.write(chunk)
        self.finish()


class DataHandlerCS(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self, endpoint_id):
        db = self.settings['db']
        db.lodex.ike.find_one({'_id': int(endpoint_id)},
                              callback=self._on_response)

    def _on_response(self, response, error):
        ss = response['ss']
        ss.update(
            {'name': response['name'], 'id': response['_id'], 'uri': response['uri']})
        chunk = createSS(ss, isCluster=True)
        self.write(chunk)
        self.finish()


class IntensionalDataHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self, s):

        id = self.get_argument("id", None)

        db.lodex.ike.find_one({'_id': int(id)}, callback=self._on_response)

    def _on_response(self, response, error):

        if error:
            raise tornado.web.HTTPError(500)
        obj = []
        tmp = {}
        tmp['s'] = self.get_argument("s", None)
        tmp['p'] = self.get_argument("p", None)
        tmp['o'] = self.get_argument("o", None)
        trovato = False
        step = 0
        ik = []
        if 'ik' in response:
            for i in response['ik']:
                # print extrValue(i['s'])
                if extrValue(i['s']) == tmp['s']:
                    trovato = True
                    tmp['ss'] = i['s']
                    ik.append(i)
                if extrValue(i['s']) == tmp['p']:
                    trovato = True
                    tmp['pp'] = i['s']
                    ik.append(i)
                if extrValue(i['s']) == tmp['o']:
                    trovato = True
                    tmp['oo'] = i['s']
                    ik.append(i)
        obj.append(tmp)
        node = []
        invNode = {}
        index = 0
        vocab = set()
        nodes = set()
        pprint.pprint(ik)
        for a in ik:
            nodes.add(a['s'])
            nodes.add(a['o'])

        for clas in nodes:
            vocab.add(extractVocab(clas))
            node.append({'name': extrValue(clas), 'fullname': clas,
                         'vocab': extractVocab(clas)})
            invNode[clas] = index
            index += 1

        edges = []
        #         pprint.pprint(response["properties"])
        for prop in ik:
            if prop['s'] in invNode and prop['o'] in invNode:
                aggiunto = False
                for e in range(len(edges)):
                    if edges[e]['source'] == invNode[prop['s']] and edges[e]['target'] == invNode[prop['o']]:
                        edges[e]['label'].append({
                            'name': extrValue(prop['p']),
                            'vocab': extractVocab(prop['p'])})
                        aggiunto = True
                if not aggiunto:
                    edges.append({'source': invNode[prop['s']],
                                  'target': invNode[prop['o']],
                                  'label': [{
                                      'name': extrValue(prop['p']),
                                      'vocab': extractVocab(prop['p'])}]})

                    #         pprint.pprint({'nodes':node,'links':edges})
                    #
                    #         pprint.pprint(vocab)
                    #
        print('lodex2')
        self.write(
            {'nodes': node, 'links': edges, 'vocab': list(vocab), 'title': response['name'], 'id': response['_id']})
        self.finish()


def extractVocab(uri):
    if len(uri.rsplit('/')[-1].split(':')) > 1:
        return ':'.join(uri.rsplit(':')[:-1])
    elif len(uri.rsplit('/')[-1].split('#')) > 1:
        return '#'.join(uri.rsplit('#')[:-1])
    else:
        return '/'.join(uri.rsplit('/')[:-1])


def extrValue(uri):
    if len(uri.rsplit('/')[-1].split(':')) > 1:
        return uri.rsplit(':')[-1]
    elif len(uri.rsplit('/')[-1].split('#')) > 1:
        return uri.rsplit('#')[-1]
    else:
        return uri.rsplit('/')[-1]


def createSS(ss, isCluster=False):
    node = []
    invNode = {}
    index = 0
    vocab = set('/'.join(a['p'].rsplit('/')[:-1]) for a in ss['attributes'])
    attributes = {}

    for att in ss['attributes']:
        #             vocab.add('/'.join(att['p'].rsplit('/')[:-1])
        if att['c'] not in attributes:
            attributes[att['c']] = [{'n': int(att['n']), 'p': att['p']}]
        else:
            attributes[att['c']].append({'n': int(att['n']), 'p': att['p']})

    for clas in ss['nodes']:
        vocab.add(extractVocab(clas['c']))
        att = []
        if clas['c'] in attributes:
            att = [{'p': extrValue(a['p']),
                    'n': float("{0:.2f}".format(float(float(a['n']) / float(clas['n'])))) if a['n'] > 0 else 0,
                    'vocab': extractVocab(a['p']), 'fullName': a['p']} for a in
                   sorted(attributes[clas['c']], key=itemgetter('n'), reverse=True)]

        currentNode = {'name': extrValue(clas['c']), 'ni': int(clas['n']), 'vocab': extractVocab(clas['c']),
                       'att': att, 'fullName': clas['c']}
        """ custer = []
        if 'cluster' in clas:
            for cl in clas['cluster']:
                currentClust = {'n': cl['n']}
                currentClust['cluster'] = [{'vocab': extractVocab(c), 'uri': c, 'name': extrValue(c)} for c in
                                           cl['cluster']]
                custer.append(currentClust)
        if len(custer) > 0:
            currentNode['cluster'] = custer """

        node.append(currentNode)
        invNode[clas['c']] = index
        index += 1

    # pprint.pprint(invNode)
    edges = []
    #         pprint.pprint(response["properties"])
    for prop in ss['edges']:
        if prop['s'] in invNode and prop['o'] in invNode:
            aggiunto = False
            for e in range(len(edges)):
                if edges[e]['source'] == invNode[prop['s']] and edges[e]['target'] == invNode[prop['o']]:
                    edges[e]['label'].append({'np': int(prop['n']),
                                              'name': extrValue(prop['p']),
                                              'vocab': extractVocab(prop['p']),
                                              'fullName': prop['p']})
                    aggiunto = True
            if not aggiunto:
                edges.append({'source': invNode[prop['s']],
                              'target': invNode[prop['o']],
                              'label': [{'np': int(prop['n']),
                                         'name': extrValue(prop['p']),
                                         'vocab': extractVocab(prop['p']),
                                         'fullName': prop['p']}]})

    for i in range(len(edges)):
        edges[i]['label'] = sorted(edges[i]['label'], key=itemgetter('np'))

    print('lodex2')

    if(not isCluster):
        chunk = {'nodes': node, 'links': edges, 'classes': None, 'classeslinks': None, 'vocab': list(vocab),
                 'title': ss['name'], 'id': ss['id'], 'uri': ss['uri']}
    else:
        '''
        initializing graph with nodes
        '''   
        g = Graph(len(node))

        '''
        add edges to the graph
        '''
        for e in edges:
            for i in range(len(e['label'])):
                g.add_edge(e['source'], e['target'])
       
        '''
        generate communities
        '''
        multi = g.community_multilevel()
        communities = []
        numCommunity = max(multi.membership) + 1
        
        '''
        assign each node to own community
        '''
        for i in range(numCommunity):
            nodesCom = []
            classesCom = []
            for n in range(len(node)):
                if multi.membership[n] == i:
                    node[n]['community'] = i
                    node[n]['id'] = n
                    nodesCom.append(node[n])
                    classesCom.append(node[n]['name'])

            currentCommunity = {'name': "", 'ni': len(nodesCom), 'vocab': "",
                                'nodes': nodesCom, 'fullName': "", 'classes': classesCom}
            communities.append(currentCommunity)
        
        """
        add edges within every community
        """
        for i in range(numCommunity):
            edgesCom = []
            for e in edges:
                source= node[e['source']]
                target= node[e['target']]
                if source['community'] == target['community'] == i:
                    edgesCom.append(
                        {'source': e['source'], 'target': e['target']})
            communities[i]['edges'] = edgesCom

        degreeNodes = [0] * len(node)
        
        """
        find the node with the higher degree which names own community
        """
        for c in communities:
            maxCount = -1
            nodeMax = None
            for e in c['edges']:                    
                if e['source'] != e['target']:
                    degreeNodes[e['source']] += 1
                    degreeNodes[e['target']] += 1
            for n in c['nodes']:
                if degreeNodes[n['id']] > maxCount:
                    maxCount = degreeNodes[n['id']]
                    nodeMax = n['id']
            c['name'] = node[nodeMax]['name']
            c['fullName'] = node[nodeMax]['name']

        linksCommunity = []

        """
        find edges between communities
        """
        for e in edges:
            source= node[e['source']]
            target= node[e['target']]
            if source['community'] != target['community']:
                linksCommunity.append(
                    {'source': source['community'], 'target': target['community']})
                   
        """
        pass parameters to html
        """        
        chunk = {'nodes': communities, 'links': linksCommunity, 'classes': node, 'classeslinks': edges,
                 'vocab': list(vocab), 'title': ss['name'], 'id': ss['id'], 'uri': ss['uri']}
    
    return chunk





if __name__ == "__main__":
    db = motor.MotorClient()
    db2 = motor.MotorClient().lodex
    application = tornado.web.Application(handlers=[
        (r"/lodex2/ok", MainHandlerOk),
        (r"/lodex2/index", IndexDatasetHandler),
        (r"/lodex2/indexComplete", IndexDatasetHandlerFull),
        (r'/lodex2/bower_components/(.*)', tornado.web.StaticFileHandler, {'path': './bower_components'}),
        (r'/bower_components/(.*)', tornado.web.StaticFileHandler, {'path': './bower_components'}),
        (r'/elements/(.*)', tornado.web.StaticFileHandler, {'path': './elements'}),
        (r'/lodex2/elements/(.*)', tornado.web.StaticFileHandler, {'path': './elements'}),
		(r'/src/(.*)', tornado.web.StaticFileHandler, {'path': './src'}),
        (r'/lodex2/src/(.*)', tornado.web.StaticFileHandler, {'path': './src'}),
        #(r'/js/(.*)', tornado.web.StaticFileHandler, {'path': './js'}),
        (r'/lodex2/js/(.*)', tornado.web.StaticFileHandler, {'path': './js'}),
        (r'/lodex2/css/(.*)', tornado.web.StaticFileHandler, {'path': './css'}),
        (r"/lodex2/([0-9]+)", GraphHandler),
        (r"/lodex2/getDataSS/([0-9]+)", DataHandlerSS),
        (r"/lodex2/getDataCS/([0-9]+)", DataHandlerCS),
        (r"/lodex2/about", About),
        (r"/lodex2/ss/([0-9]+)",SchemaSummary),
        (r"/lodex2/cs/([0-9]+)",ClusterSchema),
        #     (r"/lodex2/query", QueryDataHandler)
    ],
        static_path=os.path.join(os.path.dirname(__file__), "static"), db=db, autoreload=True, debug=True)
    port = 8891
    print('Listening on http://localhost:', port)
    application.listen(port)
    tornado.ioloop.IOLoop.instance().start()