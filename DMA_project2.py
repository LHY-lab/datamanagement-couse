import mysql.connector
import os
import csv
import surprise
import sklearn
from surprise import SVD
from surprise import SVDpp
from surprise import Dataset
from surprise import Reader
from surprise.model_selection import cross_validate
from surprise import accuracy
from surprise.model_selection import train_test_split
from surprise import KNNBasic
from surprise import BaselineOnly
from surprise import KNNWithMeans
from surprise import KNNBaseline
from surprise import KNNWithZScore
from collections import defaultdict
import numpy as np
import pandas as pd
from sklearn import tree
import graphviz
from mlxtend.frequent_patterns import association_rules, apriori

np.random.seed(0)

# TODO: CHANGE GRAPHVIZ DIRECTORY
os.environ["PATH"] += os.pathsep + 'C:/Program Files (x86)/Graphviz2.38/bin/'

# TODO: CHANGE MYSQL INFORMATION, team number 
HOST = 'localhost'
USER = 'root'
PASSWORD = 'PASSWORD'
SCHEMA = 'DMA_project2'


# PART 1: Decision tree 
def part1():
    cnx = mysql.connector.connect(host=HOST, user=USER, password=PASSWORD)
    cursor = cnx.cursor()
    cursor.execute('SET GLOBAL innodb_buffer_pool_size=2*1024*1024*1024;')
    cursor.execute('USE %s;' % SCHEMA)
    
    # TODO: Requirement 1-1. MAKE pro_mentor column
    
    pro_mentor = []

    f = open('C:\Program Files (x86)\pro_mentor\pro_mentor_list.txt', 'r', encoding="utf-8")
    while True:
        line = f.readline()
        drop_delimiter = line[0:-1]
        pro_mentor.append(drop_delimiter)
        if not line:
            break

    del pro_mentor[-1]

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pro(
        id VARCHAR(255) NOT NULL,
        PRIMARY KEY(id));
        """)

    try:
        pro_mentor_insert_query = """INSERT ignore INTO pro (id) VALUES (%s)"""
        for row in pro_mentor:
            cursor.execute(pro_mentor_insert_query, (row,))

        cnx.commit()

    except mysql.connector.Error as err:
        print('Failed to insert pro_mentor file into Mysql table{}'.format(err))

    cnx.commit()

    cursor.execute("ALTER table mentor add pro_mentor TINYINT(1) DEFAULT 0;")
    cursor.execute("""
    UPDATE mentor
    SET pro_mentor = 1
    WHERE mentor.id in
    (SELECT pro.id 
    FROM pro);""")

    cnx.commit()
    # -------
    
    
    # TODO: Requirement 1-2. WRITE MYSQL QUERY AND EXECUTE. SAVE to .csv file

    fopen = open('DMA_project2_team%02d_part1.csv' % team, 'w', encoding='utf-8')

    cursor.execute("""
    SELECT F.id, F.pro_mentor, F.age, F.have_introduction, F.have_field, count(A.id) AS num_of_answers, AVG(A.score) AS avg_of_answer_score, AVG(A.body) AS avg_of_answer_body, F.num_of_groups, F.avg_of_group_members, F.num_of_emails, F.num_of_tags
    FROM (SELECT C.id, C.pro_mentor, C.age, C.have_introduction, C.have_field, C.num_of_groups, C.avg_of_group_members, C.num_of_emails, count(T.tag_id) AS num_of_tags
    FROM (SELECT B.id, B.pro_mentor, B.age, B.have_introduction, B.have_field, B.num_of_groups, B.avg_of_group_members, count(E.id) as num_of_emails
    FROM (SELECT M.id, M.pro_mentor, (TIMESTAMPdiff(Hour, joined_date, '2020-01-01 00:00:00')) AS age, count(M.introduction) AS have_introduction, count(M.field) AS have_field, count(G.id) AS num_of_groups, AVG(G.num) AS avg_of_group_members
    FROM mentor AS M 
    LEFT JOIN (SELECT mentoring_group.id, mentoring_group.mentor, count(group_membership.mentee_id) AS num
    FROM mentoring_group 
    LEFT JOIN group_membership ON mentoring_group.id = group_membership.group_id
    GROUP BY mentoring_group.id) AS G ON M.id = G.mentor
    GROUP BY M.id) AS B 
    LEFT JOIN email AS E 
    ON B.id = E.recipient_id
    GROUP BY B.id) AS C 
    LEFT JOIN tag_mentor AS T 
    ON C.id = T.mentor_id
    GROUP BY C.id) AS F LEFT JOIN answer AS A ON F.id = A.mentor_id
    GROUP BY F.id""")


    result = cursor.fetchall()
    title = [col[0] for col in cursor.description]
    myfile = csv.writer(fopen, lineterminator='\n')
    myfile.writerow(title)
    myfile.writerows(result)

    fopen.close()

    
    # -------
    
    
    # TODO: Requirement 1-3. MAKE AND SAVE DECISION TREE
    # gini file name: DMA_project2_team##_part1_gini.pdf
    # entropy file name: DMA_project2_team##_part1_entropy.pdf

    
    df = pd.read_csv('DMA_project2_team%02d_part1.csv' % team, index_col=False)
    df = df.fillna(0)

    features = ['age', 'have_introduction', 'have_field', 'num_of_answers', 'avg_of_answer_score', 'avg_of_answer_body',
                'num_of_groups', 'avg_of_group_members', 'num_of_emails', 'num_of_tags']
    classes = ['normal', 'PRO']

    DT_gini = tree.DecisionTreeClassifier(criterion='gini', min_samples_leaf=10, max_depth=5)
    DT_gini.fit(X=df.iloc[:, 2:], y=df.iloc[:, 1])  # samples leaf ~
    graph_gini = tree.export_graphviz(DT_gini, out_file=None, feature_names=features, class_names=classes)
    graph_gini = graphviz.Source(graph_gini)
    graph_gini.render('DMA_project2_team%02d_part1_gini'% team,  view=True)

     # entropy
    DT_entropy = tree.DecisionTreeClassifier(criterion='entropy', min_samples_leaf=9, max_depth=4)
    DT_entropy.fit(X=df.iloc[:, 3:], y=df.iloc[:, 1])
    graph_entropy = tree.export_graphviz(DT_entropy, out_file=None, feature_names=list(df.columns[3:]),
                                         class_names=['normal', 'PRO'])
    graph_entropy = graphviz.Source(graph_entropy)
    graph_entropy.render('DMA_project2_team%02d_part1_entropy'% team,  view=True)
    
    # -------
    
    # TODO: Requirement 1-4. Don't need to append code for 1-4
    
    # -------
    
    cursor.close()
    

# PART 2: Association analysis
def part2():
    cnx = mysql.connector.connect(host=HOST, user=USER, password=PASSWORD)
    cursor = cnx.cursor()
    cursor.execute('SET GLOBAL innodb_buffer_pool_size=2*1024*1024*1024;')
    cursor.execute('USE %s;' % SCHEMA)
    
    # TODO: Requirement 2-1. CREATE VIEW AND SAVE to .csv file
    
    fopen = open('DMA_project2_team%02d_part2_tag.csv' % team, 'w', encoding='utf-8')

    cursor.execute('''
    CREATE VIEW tag_score AS
    SELECT tag_id, tag_name, ifnull(nmenr,0) as num_mentor, ifnull(nmnte,0) as num_mentee, ifnull(nqst,0) as num_question, ifnull(nmenr,0)+ifnull(nmnte,0)+ifnull(nqst,0) as score
    FROM
        (SELECT id as tag_id, name as tag_name
        FROM tag
        GROUP BY id) as tag
            LEFT JOIN
        (SELECT tag_id as id, count(mentor_id) as nmenr
        FROM tag_mentor
        GROUP BY tag_id) tag_mentor
            ON tag.tag_id=tag_mentor.id
            LEFT JOIN
        (SELECT tag_id as id, count(mentee_id) as nmnte
        FROM tag_mentee
        GROUP BY tag_id) tag_mentee 
            ON tag.tag_id=tag_mentee.id
            LEFT JOIN
        (SELECT tag_id as id, count(question_id) as nqst
        FROM tag_question
        GROUP BY tag_id) tag_question
        ON tag.tag_id=tag_question.id
    ORDER BY score DESC LIMIT 50;
    ''')

    cursor.execute('''
    SELECT "tag_id", "tag_name", "num_mentor", "num_mentee", "num_question", "score" UNION SELECT * INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/DMA_project2_team03_part2_tag.csv'
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    FROM tag_score;
    ''')
    
    fopen.close()
    
    # ------
    
    # TODO: Requirement 2-2. CREATE 2 VIEWS AND SAVE partial one to .csv file 
    
    fopen = open('DMA_project2_team%02d_part2_UIR.csv' % team, 'w', encoding='utf-8')

    cursor.execute('''
    CREATE VIEW user_item_rating as
    SELECT * FROM 
    (SELECT  user_id as user,tag_name as item, ifnull(check_follow,0)*5+if(count>5, 5, ifnull(count,0)) as rating FROM
        ((SELECT a.tag_id as tag_id, a.tag_name as tag_name, b.id as user_id 
        FROM tag_score as a 
        JOIN
       (Select mentor_id as id from tag_mentor 
       union select mentee_id as id from tag_mentee 
       union select mentee_id as id from question 
       union select mentor_id as id from answer) as b)) list
        LEFT JOIN
        (SELECT tag_question.tag_id as tag_id1, tag_question.question_id, ifnull(count(question.id),0) as count,
        question.mentee_id as user_id1
        FROM tag_question
        LEFT JOIN question
        ON tag_question.question_id=question.id
        GROUP BY tag_id1, user_id1
        UNION
        SELECT tag_question.tag_id as tag_id1, tag_question.question_id, ifnull(count(answer.question_id),0) as count, 
        answer.mentor_id as user_id1
        FROM tag_question
        LEFT JOIN answer
        ON tag_question.question_id=answer.question_id
        GROUP BY tag_id1, user_id1) numqa
        ON tag_id=tag_id1 and user_id=user_id1
        LEFT JOIN
        (SELECT tag_id as tag_id2, mentee_id as user_id2, 1 as check_follow FROM tag_mentee
        UNION SELECT tag_id as tag_id2, mentor_id as user_id2, 1 as check_follow FROM tag_mentor
        GROUP BY tag_id2, user_id2) as follow 
        ON tag_id=tag_id2 and user_id=user_id2) total
    WHERE rating>=1;
    ''')

    cursor.execute('''
    CREATE VIEW partial_user_item_rating AS
    SELECT user, item, rating FROM
    (SELECT cnt1.user as us, cnt1.cnt
    FROM (SELECT user, count(rating) as cnt FROM user_item_rating 
	GROUP BY user) as cnt1
    WHERE cnt1.cnt >=4) cnt2
    INNER JOIN
    (SELECT item, user, rating FROM user_item_rating) as uir
    ON uir.user=cnt2.us;
    ''')

    
    cursor.execute('''
    SELECT "user","item", "rating" UNION SELECT * INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/DMA_project2_team03_part2_UIR.csv'
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    FROM partial_user_item_rating;
    ''')
    
    fopen.close()
    
    # ------
    
     # TODO: Requirement 2-3. MAKE HORIZONTAL VIEW
    
    df = pd.read_csv('DMA_project2_team%02d_part2_UIR.csv' % team)
    ids = df.user.unique().tolist()
    tags = df.item.unique().tolist()

    data = pd.DataFrame(index=ids, columns=tags)

    # Convert to Sparse Matrix
    for id in ids:
        idCol = df[df.user==id]
        for tag in tags:
            value = 1 if idCol[df.item == tag].any().rating else 0
            data.loc[id, tag] = value

    data.to_pickle('DMA_project2_team%02d_part2_horizontal.pkl' % team)




    # ------

    
    # TODO: Requirement 2-4. ASSOCIATION ANALYSIS
     df = pd.read_pickle('DMA_project2_team%02d_part2_horizontal.pkl' % team)

    cols = df.columns
    for col in cols:
        df[col] = df[col].apply(lambda x: True if x == 1 else False)

    # Get Frequent itemset
    frequent_itemset = apriori(df, min_support=0.01, use_colnames=True) # If memory is not enough, use "frequent_itemset = apriori(df, min_support=0.01, use_colnames=True, low_memory=True)"

    # Association rules (by metric "Lift")
    data = association_rules(frequent_itemset, metric="lift", min_threshold=1)
    data.to_pickle('DMA_project2_team%02d_part2_association.pkl' % team)
    
    # High lift
    df_lift = data.sort_values(by='lift', ascending=False).head(10)
    df_lift.to_csv("top10lift.csv")
    
    # ------
    
    cursor.close()
    

# TODO: Requirement 3-1. WRITE get_top_n 
def get_top_n(algo, testset, id_list, n=10, user_based=True):

    results = defaultdict(list)
    if user_based:
        testset_id = [x for x in testset if (x[0] in id_list)]
        predictions = algo.test(testset_id)
        for uid, iid, true_r, est, _ in predictions:
            results[uid].append((iid, est))
            pass
    else:
        testset_id = [x for x in testset if (x[1] in id_list)]
        predictions = algo.test(testset_id)
        for uid, iid, true_r, est, _ in predictions:
            results[iid].append((uid, est))
            pass
    for uid, ratings in results.items():
        ratings.sort(key=lambda x: x[1], reverse=True)
        results[uid] = ratings[:n]
        pass

    return results


# PART 3. Requirement 3-2, 3-3, 3-4
def part3():
    file_path = ''
    reader = Reader(line_format='user item rating', sep=',', rating_scale=(1, 10), skip_lines=1)
    data = Dataset.load_from_file(file_path, reader=reader)
    trainset = data.build_full_trainset()
    testset = trainset.build_anti_testset()

    ##3-2
    uid_list = ['ffffbe8d854a4a5a8ab1a381224f5b80',
                'ffe2f26d5c174e13b565d026e1d8c503',
                'ffdccaff893246519b64d76c3561d8c7',
                'ffdb001850984ce69c5f91360ac16e9c',
                'ffca7b070c9d41e98eba01d23a920d52']

    sim_options = {'name': 'cosine', 'user_based': True}
    algo = surprise.KNNBasic(sim_options=sim_options)
    algo.fit(trainset)
    results = get_top_n(algo, testset, uid_list, n=10, user_based=True)
    with open('3-2-1.txt', 'w') as f:
        for uid, ratings in sorted(results.items(), key=lambda x: x[0]):
            f.write('User ID %s top-10 results\n' % uid)
            for iid, score in ratings:
                f.write('Item ID %s\tscore %s\n' % (iid, str(score)))
            f.write('\n')

    algo = surprise.KNNWithMeans(sim_options=sim_options)
    algo.fit(trainset)
    results = get_top_n(algo, testset, uid_list, n=10, user_based=True)

    algo = surprise.KNNWithZScore(sim_options=sim_options)
    algo.fit(trainset)
    results = get_top_n(algo, testset, uid_list, n=10, user_based=True)

    algo = surprise.KNNBaseline(sim_options=sim_options)
    algo.fit(trainset)
    results = get_top_n(algo, testset, uid_list, n=10, user_based=True)

    sim_options2 = {'name': 'pearson', 'user_based': True}
    algo = surprise.KNNWithMeans(sim_options2=sim_options2)
    algo.fit(trainset)
    results = get_top_n(algo, testset, uid_list, n=10, user_based=True)
    with open('3-2-2.txt', 'w') as f:
        for uid, ratings in sorted(results.items(), key=lambda x: x[0]):
            f.write('User ID %s top-10 results\n' % uid)
            for iid, score in ratings:
                f.write('Item ID %s\tscore %s\n' % (iid, str(score)))
            f.write('\n')

    algo = surprise.KNNBasic(sim_options2=sim_options2)
    algo.fit(trainset)
    results = get_top_n(algo, testset, uid_list, n=10, user_based=True)

    algo = surprise.KNNWithZScore(simoptions2=sim_options2)
    algo.fit(trainset)
    results = get_top_n(algo, testset, uid_list, n=10, user_based=True)

    algo = surprise.KNNBaseline(sim_options2=sim_options2)
    algo.fit(trainset)
    results = get_top_n(algo, testset, uid_list, n=10, user_based=True)

    best_algo_ub = cross_validate(algo, data, cv=5, verbose=True)

    
    ##3-3
    iid_list = ['art', 'teaching', 'career', 'college', 'medicine']
    sim_options3 = {'name': 'cosine', 'user_based': False}
    algo = surprise.KNNBasic(sim_options3=sim_options3)
    algo.fit(trainset)
    results = get_top_n(algo, testset, iid_list, n=10, user_based=False)
    with open('3-3-1.txt', 'w') as f:
        for iid, ratings in sorted(results.items(), key=lambda x: x[0]):
            f.write('Item ID %s top-10 results\n' % iid)
            for uid, score in ratings:
                f.write('User ID %s\tscore %s\n' % (uid, str(score)))
            f.write('\n')
    algo = surprise.KNNWithMeans(sim_options3=sim_options3)
    algo.fit(trainset)
    results = get_top_n(algo, testset, iid_list, n=10, user_based=False)

    algo = surprise.KNNWithZScore(sim_options3=sim_options3)
    algo.fit(trainset)
    results = get_top_n(algo, testset, iid_list, n=10, user_based=False)

    algo = surprise.KNNBaseline(sim_options3=sim_options3)
    algo.fit(trainset)
    results = get_top_n(algo, testset, iid_list, n=10, user_based=False)
    best_algo_ib = cross_validate(algo, data, cv=5, verbose=True)

    sim_options4 = {'name': 'pearson', 'user_based': False}
    algo = KNNWithMeans(sim_options4=sim_options4)
    algo.fit(trainset)
    results = get_top_n(algo, testset, iid_list, n=10, user_based=False)
    with open('3-3-2.txt', 'w') as f:
        for iid, ratings in sorted(results.items(), key=lambda x: x[0]):
            f.write('Item ID %s top-10 results\n' % iid)
            for uid, score in ratings:
                f.write('User ID %s\tscore %s\n' % (uid, str(score)))
            f.write('\n')

    algo = KNNWithZScore(sim_options4=sim_options4)
    algo.fit(trainset)
    results = get_top_n(algo, testset, iid_list, n=10, user_based=False)

    algo = KNNBaseline(sim_options4=sim_options4)
    algo.fit(trainset)
    results = get_top_n(algo, testset, iid_list, n=10, user_based=False)

    algo = KNNBasic(sim_options4=sim_options4)
    algo.fit(trainset)
    results = get_top_n(algo, testset, iid_list, n=10, user_based=False)


    ##3-4
    algo = surprise.SVD(n_factors=100, n_epochs=50, biased=False)
    algo.fit(trainset)
    results = get_top_n(algo, testset, uid_list, n=10, user_based=True)
    with open('3-4-1.txt', 'w') as f:
        for uid, ratings in sorted(results.items(), key=lambda x: x[0]):
            f.write('User ID %s top-10 results\n' % uid)
            for iid, score in ratings:
                f.write('Item ID %s\tscore %s\n' % (iid, str(score)))
            f.write('\n')

    algo = surprise.SVD(n_factors=200, n_epochs=100, biased=True)
    algo.fit(trainset)
    results = get_top_n(algo, testset, uid_list, n=10, user_based=True)
    with open('3-4-2.txt', 'w') as f:
        for uid, ratings in sorted(results.items(), key=lambda x: x[0]):
            f.write('User ID %s top-10 results\n' % uid)
            for iid, score in ratings:
                f.write('Item ID %s\tscore %s\n' % (iid, str(score)))
            f.write('\n')
    algo = surprise.SVD(n_factors=100, n_epochs=100, biased=True)
    algo.fit(trainset)
    results = get_top_n(algo, testset, uid_list, n=10, user_based=True)

    algo = surprise.SVD(n_factors=200, n_epochs=200, biased=True)
    algo.fit(trainset)
    results = get_top_n(algo, testset, uid_list, n=10, user_based=True)

    algo = surprise.SVDpp(n_factors=100, n_epochs=50)
    algo.fit(trainset)
    results = get_top_n(algo, testset, uid_list, n=10, user_based=True)
    with open('3-4-3.txt', 'w') as f:
        for uid, ratings in sorted(results.items(), key=lambda x: x[0]):
            f.write('User ID %s top-10 results\n' % uid)
            for iid, score in ratings:
                f.write('Item ID %s\tscore %s\n' % (iid, str(score)))
            f.write('\n')

    algo = surprise.SVDpp(n_factors=100, n_epochs=100)
    algo.fit(trainset)
    results = get_top_n(algo, testset, uid_list, n=10, user_based=True)
    with open('3-4-4.txt', 'w') as f:
        for uid, ratings in sorted(results.items(), key=lambda x: x[0]):
            f.write('User ID %s top-10 results\n' % uid)
            for iid, score in ratings:
                f.write('Item ID %s\tscore %s\n' % (iid, str(score)))
            f.write('\n')

    algo = surprise.SVDpp(n_factors=150, n_epochs=50)
    algo.fit(trainset)
    results = get_top_n(algo, testset, uid_list, n=10, user_based=True)

    algo = surprise.SVDpp(n_factors=150, n_epochs=100)
    algo.fit(trainset)
    results = get_top_n(algo, testset, uid_list, n=10, user_based=True)
    best_algo_mf = cross_validate(algo, data, cv=5, verbose=True)    
    

if __name__ == '__main__':
    part1()
    part2()
    part3()


# In[ ]:




