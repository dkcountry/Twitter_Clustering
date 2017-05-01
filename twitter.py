import sqlite3
import tweepy
import networkx as nx
import matplotlib.pyplot as plt
import random
import csv
import time
from matplotlib.backends.backend_pdf import PdfPages


###### Authenticate twitter access and initiate SQL database ######

#NOTE - This function requires oauth setup on twitter's website
#       Don't use if database has already been created and the data is downloaded
def oauth():
    consumer_key="G6SiCzmqjUKGZbaByqzPbg"
    consumer_secret="hLuXoVJwvmP1pOUkwIrSXx88yeWgbJWA6LBVq1sweo"
    access_token="547876623-3Glp21Z5nAOA7iTRZT7OqFhUkelvwJYzHJ3vKV4M"
    access_token_secret="adbRSBPsOQsUkKGjFzVJG7g4L6QG3Gwpt2IheuU4qi8"   

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)
    return api

#Create database
def db(database):
    conn = sqlite3.connect(database)
    return conn

#Create these tables on initial run
def initiate_db():
    c.execute('''create table tweets(Create_At datetime, From_User text, From_User_ID int, From_User_ID_Str text, ID text, ID_Str text, Source text, Text text)''')
    c.execute('''create table Edges(Stock1 text, Stock2 text, Weight int)''')
    c.execute('''create table Nodes(Stock text)''')

###### Data Scrape Code ######
    
# stock_list - for testing uses
DOW = ['AA Alcoa', 'AXP American Express', 'BA Boeing', 'BAC Bank of America',
       'CAT Caterpillar', 'CSCO Cisco', 'CVX CHevron', 'DD Du Pont', 'DIS Walt Disney',
       'GE General Electric', 'HD Home Depot', 'HPQ Hewlett-Packard', 'IBM International Business Machines',
       'INTC Intel', 'JNJ Johnson & Johnson', 'JPM JP Morgan', 'KO Coca-Cola', "MCD McDonald's",
       'MMM 3M', 'MRK Merck', 'MSFT Microsoft', 'PFE Pfizer', 'PG Procter & Gamble',
       'T AT&T', 'TRV Travelers', 'UNH UnitedHealth', 'UTX United Technologies', 'VZ Verizon', 'WMT Wal-Mart',
       'XOM Exxon Mobil']

# Upload stock_list from external csv file
def uploadtickers(filename):
    upload = open(filename,"rU")
    csvreader = csv.reader(upload)    
    stock_list = [str(x[0]) for x in csvreader]
    return stock_list

# this function requires your oauth password keys
def retrievetweets(stock_list): #input - list of strings e.g. ("BA Boeing", ..)
    for x in stock_list:
        print x
        n = 0
        for i in range(1,6,1):
            a = api.search(x,"en","ja",100,i)   
            try:
                for x in a:
                    created_at = x.created_at
                    from_user = x.from_user
                    from_user_id = x.from_user_id
                    from_user_id_str = x.from_user_id_str
                    d = x.id
                    id_str = x.id_str
                    source = x.source
                    text = x.text
                    c.execute("insert into tweets values (?, ?, ?, ?, ?, ?, ?, ?)", (created_at, from_user, from_user_id, from_user_id_str, d, id_str, source, text))
                    conn.commit()
                    n = n+1
            except sqlite3.InterfaceError:
                import pdb; pdb.set_trace()
        print "Number of tweets retrieved: " + str(n)

###### Graph Analysis Begin ######

#Build raw graph from data
def fillgraph(stock_list):
    print "Generating initial graph."
    for i in range(len(stock_list)):
        print "Finding edges for: "+stock_list[i]
        c.execute("INSERT INTO Nodes VALUES (?)", (stock_list[i],))
        j = i+1
        while (j > i and j < len(stock_list)):
            sentence1 = stock_list[i].split()
            sentence2 = stock_list[j].split()
            c.execute(r"select Text from Tweets where (Text LIKE ? OR Text LIKE ?) AND (Text LIKE ? OR Text LIKE ?)", ('%$'+sentence1[0]+'%', '%$'+" ".join(sentence1[1:])+'%','%$'+sentence2[0]+'%','%$'+" ".join(sentence2[1:])+'%'))
            weight = len(c.fetchall())
            c.execute("INSERT INTO Edges VALUES (?, ?, ?)", (stock_list[i], stock_list[j], weight))
            conn.commit()
            j = j+1
        conn.commit()

#Implement k-core algorithm to find initial clusters 
def create_k_core(stock_list,k,minweight):
    iterate = True
    n = 0
    print "Generating k-core clusters; k=" +str(k)
    while (iterate):
        print "iteration: " + str(n) 
        iterate = k_iteration(stock_list,k,minweight)
        n = n+1
    c.execute("drop table new_edges")
    c.execute("drop table new_nodes")
    conn.commit()
    

#Iterate until the graph is stable
#Return true if stable and false otherwise
def k_iteration(stock_list,k,minweight):

    #check if the sql tables already exist
    try:
        c.execute("select * from temp_Nodes")
    except:
        c.executescript("""
        create table new_Nodes (stock text);
        create table new_Edges (stock1 text, stock2 text, weight int);
        create table temp_Nodes (stock text);
        create table temp_Edges (stock1 text, stock2 text, weight int);
        insert into temp_Nodes select * from Nodes;
        insert into temp_Edges select * from Edges;
        """)

    #fetch the nodes and check that each node has degree >= k
    c.execute('select count(*) from temp_nodes')
    oldnum = c.fetchall()[0][0]
    for x in stock_list:
        c.execute('''select * from temp_Edges where (stock1 = ? or stock2 =
                  ?) and weight > ?''', (x,x,minweight))
        result = c.fetchall()
        if len(result) >= k:
            c.execute("insert into new_Nodes Values (?)", (x,))
            c.execute('''insert into new_Edges select * from temp_edges
                 where (stock1 = ? or stock2 = ?) and weight > ?
                 ''', (x,x,minweight))
    c.executescript('''
        drop table temp_Nodes;
        drop table temp_Edges;
        create table temp_Nodes (stock text);
        create table temp_Edges (stock1 text, stock2 text, weight int);
        insert into temp_Nodes select distinct * from new_Nodes;
        insert into temp_Edges select distinct * from new_Edges;
        drop table new_Nodes;
        drop table new_Edges;
        create table new_Nodes (stock text);
        create table new_Edges (stock1 text, stock2 text, weight int);
        ''')
    c.execute('select count(*) from temp_nodes')
    newnum = c.fetchall()[0][0]
    c.execute('select count(*) from temp_edges')
    newedges = c.fetchall()[0][0]
    print "new # of nodes: " + str(newnum)
    print "new # of edges: " + str(newedges)
    if newnum < oldnum:
        return True
    else:
        print "Stable."
        return False

#Implement k-community algorithm to enhance cluster results 
        
def create_k_comm(stock_list,k,graph_on):
    iterate = True
    n = 1
    print "Generating k-community clusters; k = " +str(k)
    while (iterate):
        print "Iteration: "+str(n)
        iterate = k_comm_iteration(stock_list,k)
        if (graph_on):
            interactive_graph(1,k,n)
        n = n+1
    c.execute("drop table new_k_comm_edges")
    c.execute("drop table new_k_comm_nodes")
    conn.commit()

#Iterate until the graph is stable
#Return true if stable and false otherwise
def k_comm_iteration(stock_list,k): #reminder: all k-communities are k+1 cores
    #if first iteration: create data tables
    try:
        c.execute("select * from k_comm_nodes")
    except:
        c.executescript("""
        create table new_k_comm_nodes (stock text);
        create table new_k_comm_edges (stock1 text, stock2 text, weight int);
        create table k_comm_nodes (stock text);
        create table k_comm_edges (stock1 text, stock2 text, weight int);
        insert into k_comm_nodes select * from temp_Nodes;
        insert into k_comm_edges select * from temp_Edges;
        """)
    c.execute('select count(*) from k_comm_nodes')
    oldnum = c.fetchall()[0][0]
    
    #count num of entries in k_comm_edges    
    c.execute("select count(*) from k_comm_edges")
    entries = c.fetchall()[0][0]
    #for each entry, implement the k-comm algorithm
    for i in range(entries):
        c.execute("select * from k_comm_edges limit 1 offset ?", (i,))
        current = c.fetchall()[0]
        #vertices contained in this particular edge
        stock1 = current[0]
        stock2 = current[1]
        c.execute("select * from k_comm_edges where stock1 = ? or stock2 = ?" , (stock1,stock1))
        raw1 = c.fetchall()
        c.execute("select * from k_comm_edges where stock1 = ? or stock2 = ?" , (stock2,stock2))
        raw2 = c.fetchall()
        neighbors1 = []
        for x in raw1:
            if x[0] == stock1:
                neighbors1.append(x[1])
            else:
                neighbors1.append(x[0])
        neighbors2 = []
        for x in raw2:
            if x[0] == stock2:
                neighbors2.append(x[1])
            else:
                neighbors2.append(x[0])
        shared_neighbors = 0
        for x in neighbors1:
            for y in neighbors2:
                if x == y:
                    shared_neighbors = shared_neighbors + 1
        if shared_neighbors >= k:
            c.execute("insert into new_k_comm_nodes Values (?)", (stock1,))
            c.execute("insert into new_k_comm_nodes Values (?)", (stock2,))
            c.execute('''insert into new_k_comm_edges select * from k_comm_edges
                     where (stock1 = ? or stock2 = ?) and (stock1 = ? or stock2 = ?)''',
                      (stock1,stock1,stock2,stock2))
    c.executescript('''
        drop table k_comm_nodes;
        drop table k_comm_edges;
        create table k_comm_nodes (stock text);
        create table k_comm_edges (stock1 text, stock2 text, weight int);
        insert into k_comm_nodes select distinct * from new_k_comm_nodes;
        insert into k_comm_edges select distinct * from new_k_comm_edges;
        drop table new_k_comm_nodes;
        drop table new_k_comm_edges;
        create table new_k_comm_nodes (stock text);
        create table new_k_comm_edges (stock1 text, stock2 text, weight int);
        ''')
    c.execute('select count(*) from k_comm_nodes')
    newnum = c.fetchall()[0][0]
    c.execute('select count(*) from k_comm_edges')
    newedges = c.fetchall()[0][0]
    print "new # of nodes: " + str(newnum)
    print "new # of edges: " + str(newedges)
    if newnum < oldnum:
        return True
    else:
        print "Stable."
        return False

#Drop k-core tables - used during testing    
def droptables():
    try:
        c.execute("drop table temp_Nodes")
        c.execute("drop table temp_Edges")
    except:
        None
    try:
        c.execute("drop table new_Nodes")
        c.execute("drop table new_Edges")
    except:
        None
    

#Drop k-community tables 
def droptables2():
    try:
        c.execute("drop table new_k_comm_nodes")
        c.execute("drop table new_k_comm_edges")
    except:
        None
    try:
        c.execute("drop table k_comm_nodes")
        c.execute("drop table k_comm_edges")
    except:
        None

#cleans the names in each entry of the clusters
def strip_cluster_list(cluster_list):
    new_cluster_list = []
    for x in cluster_list:
        temp_list = []
        for y in x:
            newy = y.split()[0]
            temp_list.append(newy)
        new_cluster_list.append(temp_list)
    return new_cluster_list

#find all the clusters the k-community graph              
def findall_clusters():
    cluster_list = []
    iteration = 0
    proceed = True
    c.executescript("""CREATE TABLE copy_edges(stock1 text, stock2 text, weight int);
                        insert into copy_edges select * from k_comm_edges;
                        """)
    while (proceed):
        c.execute("select * from copy_edges")
        try:
            c.fetchall()[0]
        except:
            break
        cluster_list.append(cluster())
        iteration = iteration + 1
    c.execute("drop table copy_edges")
    return cluster_list
    
#returns an individual cluster
def cluster():
    node_list = []
    proceed = True
    c.execute("create table cluster_nodes (stock text)")
    while (node_list != None or proceed):
        node_list = cluster_iteration(node_list)
        proceed = False
    c.execute("select distinct * from cluster_nodes")
    cluster = [x[0] for x in c.fetchall()]
    
    c.execute("drop table cluster_nodes")
    for x in cluster:
        c.execute("delete from copy_edges where (stock1 = ? or stock2 =?)",(x,x))
    return cluster

#iterate until a complete cluster has been established
def cluster_iteration(node_list): 
    temp_list = []
    c.execute("select * from copy_edges limit 1")
    try:
        entry = c.fetchall()[0]
    except:
        return
    stock1 = entry[0]
    stock2 = entry[1]
    temp_list.append(stock1)
    c.execute("select * from copy_edges where (stock1 = ? or stock2 = ?)", (stock1,stock1))
    match_entries = c.fetchall()
    for y in match_entries:
        if y[0] == stock1:
            temp_list.append(y[1])
        else:
            temp_list.append(y[0])
    c.execute("delete from copy_edges where (stock1 = ? or stock2 = ?)", (stock1,stock1))
    for y in temp_list:
        c.execute("insert into cluster_nodes Values(?)",(y,))
        if y not in node_list:
            node_list.append(y)

# MAIN FUNCTION TO CALL FOR K COMMUNITY VISUALIZATION
def k_comm_multipleruns(stock_list,k,graph_on):

    #create output pdf
    if graph_on:
        global pp
        pp = PdfPages('Output.pdf')
    for i in range(1,k+1,1):
        droptables2()
        create_k_comm(stock_list,i,graph_on)
    if graph_on:
        interactive_graph(2,None,None)
            
###### END Graph Analysis ######

###### Graphing utilities ######
#Color Dictionary - copied from networkx documentation
#Nodes colors will be randomly generated from this list
color_list = ['aliceblue', 'antiquewhite', 'aqua', 'aquamarine', 'azure', 'beige', 'bisque', 'black', 'blanchedalmond', 'blue', 'blueviolet', 'brown', 'burlywood', 'cadetblue', 'chartreuse', 'chocolate', 'coral', 'cornflowerblue', 'cornsilk', 'crimson', 'cyan', 'darkblue', 'darkcyan',
'darkgoldenrod', 'darkgray', 'darkgreen', 'darkkhaki', 'darkmagenta', 'darkolivegreen', 'darkorange', 'darkorchid', 'darkred', 'darksalmon', 'darkseagreen', 'darkslateblue', 'darkslategray', 'darkturquoise', 'darkviolet', 'deeppink', 'deepskyblue', 'dimgray','dodgerblue', 'firebrick',
'floralwhite', 'forestgreen', 'fuchsia', 'gainsboro', 'ghostwhite', 'gold', 'goldenrod', 'gray', 'green', 'greenyellow', 'honeydew', 'hotpink', 'indianred', 'indigo', 'ivory', 'khaki', 'lavender', 'lavenderblush', 'lawngreen', 'lemonchiffon', 'lightblue', 'lightcoral', 'lightcyan',
'lightgoldenrodyellow', 'lightgreen','lightpink', 'lightsalmon', 'lightseagreen', 'lightskyblue', 'lightslategray', 'lightsteelblue', 'lightyellow', 'lime', 'limegreen', 'linen', 'magenta', 'maroon', 'mediumaquamarine', 'mediumblue', 'mediumorchid', 'mediumpurple',
'mediumseagreen', 'mediumslateblue', 'mediumspringgreen', 'mediumturquoise', 'mediumvioletred', 'midnightblue', 'mintcream', 'mistyrose', 'moccasin', 'navajowhite', 'navy', 'oldlace', 'olive', 'olivedrab', 'orange', 'orangered', 'orchid', 'palegoldenrod', 'palegreen',
'palevioletred', 'papayawhip', 'peachpuff', 'peru', 'pink', 'plum', 'powderblue', 'purple', 'red', 'rosybrown', 'royalblue', 'saddlebrown', 'salmon', 'sandybrown', 'seagreen', 'seashell', 'sienna', 'silver', 'skyblue', 'slateblue', 'slategray', 'snow', 'springgreen', 'steelblue',
'tan', 'teal', 'thistle', 'tomato', 'turquoise', 'violet', 'wheat', 'white', 'whitesmoke', 'yellow', 'yellowgreen']

#Draw raw graph - no cluster identification
def graph():
    print "Display original graph."
    G = nx.Graph()
    c.execute("select * from k_comm_nodes")
    nodes = [x[0] for x in c.fetchall()]
    ticker = []
    for x in nodes:
        node = x.split()
        ticker.append(node[0])
    c.execute("select stock1, stock2 from k_comm_edges")
    edges = [x for x in c.fetchall()]
    newedges = []
    for x in edges:
        edge1 = x[0].split()[0]
        edge2 = x[1].split()[0]
        newedges.append([edge1,edge2])
    G.add_nodes_from(ticker)
    G.add_edges_from(newedges)
    nx.draw(G)
    plt.show()

#Draw graph and separate clusters by color
def graph_clusters():
    print "Displaying Graph with clusters identified"
    G = nx.Graph()
    c.execute("select * from k_comm_nodes")
    nodes = [x[0] for x in c.fetchall()]
    ticker = []
    for x in nodes:
        node = x.split()
        ticker.append(node[0])
    c.execute("select stock1, stock2 from k_comm_edges")
    edges = [x for x in c.fetchall()]
    newedges = []
    for x in edges:
        edge1 = x[0].split()[0]
        edge2 = x[1].split()[0]
        newedges.append([edge1,edge2])
    G.add_nodes_from(ticker)
    G.add_edges_from(newedges)
    cluster_list = findall_clusters()
    cluster_list = strip_cluster_list(cluster_list)
    cluster_colors = []
    colors = []
    for x in cluster_list:
        color = random.choice(color_list)
        cluster_colors.append((x, color))
    for y in G.nodes():
        for x in cluster_colors:
            if y in x[0]:
                colors.append(x[1])
    plt.title("")
    nx.draw(G, node_size = 500, alpha = 0.5, width = 2, node_color = colors, edge_color = "gray", font_size = 10)
    plt.draw()
    plt.show()

#continuously update the graph with each iteration
def interactive_graph(mode,k,n):
    
    #mode 1 - open the interactive plot
    if mode == 1:
        plt.ion()
        graph_clusters()
        plt.title("K community = " + str(k) + " Iteration = " + str(n))
        plt.pause(.01)
        try:
            pp.savefig()
        except:
            None
        time.sleep(2)
        plt.clf()

    #close the graph
    if mode == 2:
        plt.ioff()
        plt.close()
        try:
            pp.close()
        except:
            None

###### GUI ######
class App:
    def __init__(self, master):
        
        #initiate the gui box
        bg_color = 'lightslategray'
        fm = Frame(master, width = 300, height = 200, bg = bg_color, highlightbackground = 'blue')
        fm.pack(side = TOP, expand = NO, fill = NONE)
        self.body(fm)
        self.buttonbox()
    

    def body(self, master):
        # labels 
        bg_color = 'lightslategray'
        fg_color = 'gainsboro'
        Label(master, text="Twitter Graph Analysis", bg = bg_color, fg = fg_color).grid(row=0,sticky=E)
        Label(master, text="David Kang and Sally Qizhen He", bg = bg_color, fg = fg_color).grid(row=0,column=3,columnspan = 3,sticky=W)

        Label(master, text="Data File: ", bg = bg_color, fg = fg_color).grid(row=1, sticky=E)
        Label(master, text="Database: ", bg = bg_color, fg = fg_color).grid(row=2,sticky=E)
        Label(master, text="K-Level: ", bg = bg_color, fg = fg_color).grid(row=3,sticky=E)
        Label(master, text="Min Weight per Node: ", bg = bg_color, fg = fg_color).grid(row=4, sticky=E)

        Label(master, text="Scrap Data", bg = bg_color, fg = fg_color).grid(row=5, sticky=E)
        Label(master, text="K-Core Analysis", bg = bg_color, fg = fg_color).grid(row=6, sticky=E)
        Label(master, text="K-Community Analysis", bg = bg_color, fg = fg_color).grid(row=7, sticky=E)
    
 
        # text boxes
        self.datafile = Entry(master,width=20, bg = fg_color, bd = 0.0)
        self.database = Entry(master,width=20, bg = fg_color, bd = 0.0)
        self.klevel = Entry(master,width=5, bg = fg_color, bd = 0.0)
        self.minw = Entry(master,width=5, bg = fg_color, bd = 0.0)

        self.datafile.grid(row = 1, column = 1, columnspan = 2, sticky=W)
        self.database.grid(row = 2, column = 1, columnspan = 2, sticky=W)
        self.klevel.grid(row = 3, column = 1, columnspan = 2, sticky=W)
        self.minw.grid(row = 4, column = 1, columnspan = 2, sticky=W)

        #check boxes
        self.scrap = IntVar(master)
        Checkbutton(master, variable=self.scrap, bg = bg_color).grid(row=5,column=1,sticky=W)
        self.kcore = IntVar(master)
        Checkbutton(master, variable=self.kcore, bg = bg_color).grid(row=6,column=1,sticky=W)
        self.kcommunity = IntVar(master)
        Checkbutton(master, variable=self.kcommunity, bg = bg_color).grid(row=7,column=1,sticky=W)
        
    def buttonbox(self):       
        box = Frame()
        w = Button(box, text = "RUN", width = 10, command=self.run)
        w.pack(side=LEFT, padx = 5, pady = 5)
        w = Button(box, text = "CLEAR", width = 10, command=self.clear_all)
        w.pack(side=LEFT, padx = 5, pady = 5)
        box.pack()

    def run(self, event=None):
        # pull all necessary fields
        datafile = self.datafile.get()
        database = self.database.get()
        klevel = self.klevel.get()
        minw = self.minw.get()
        scrap = self.scrap.get()
        kcore = self.kcore.get()
        kcommunity = self.kcommunity.get()

        #Initiate global connections
        global conn
        global c
        conn = db(str(database))
        c = conn.cursor()       
        stock_list = uploadtickers(str(datafile))

        #RUN
        if scrap == 1:
            initiate_db()
            global api
            api = oauth()
            retrievetweets(stock_list)
            fillgraph(stock_list)
        if kcore == 1:
            droptables()
            create_k_core(stock_list,1,int(minw))
        if kcommunity == 1:
            k_comm_multipleruns(stock_list, int(klevel), True)
        conn.close()

    #clear all the fields
    def clear_all(self, event=None):
        self.datafile.delete(0,END)
        self.database.delete(0,END)
        self.klevel.delete(0,END)
        self.minw.delete(0,END)
        self.scrap.set(0)
        self.kcore.set(0)
        self.kcommunity.set(0)
        
##############################

#start gui when program is opened
from Tkinter import *
root = Tk()
app = App(root)
root.mainloop()

