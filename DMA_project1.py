import mysql.connector

# TODO: REPLACE THE VALUE OF VARIABLE team (EX. TEAM 1 --> team = 1)


# Requirement1: create schema ( name: DMA_team## )
def requirement1(host, user, password):
    cnx = mysql.connector.connect(host=host, user=user, password=password, use_pure=True)
    cursor = cnx.cursor()
    cursor.execute('SET GLOBAL innodb_buffer_pool_size=2*1024*1024*1024;')

    # TODO: WRITE CODE HERE
    cursor.execute("""DROP DATABASE IF EXISTS DMA_project1;""")
    cursor.execute("""CREATE DATABASE DMA_project1 CHARACTER SET = utf8 ;""")
    cursor.execute("""USE DMA_project1""")


# TODO: WRITE CODE HERE


# Requirement2: create table
def requirement2(host, user, password):
    cnx = mysql.connector.connect(host=host, user=user, password=password, use_pure=True)
    cursor = cnx.cursor()
    cursor.execute("""USE DMA_project2_team03""")

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS answer(
    id VARCHAR(255) NOT NULL,
    mentor_id VARCHAR(255) NOT NULL,
    question_id VARCHAR(255) NOT NULL,
    answered_date DATETIME NOT NULL,
    body INT(11) NOT NULL,
    score INT(11) NOT NULL,
    PRIMARY KEY (id))
    Engine = InnoDB DEFAULT CHARSET = utf8mb4;
    ''')

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS mentee(
    id VARCHAR(255) NOT NULL,
    district VARCHAR(255),
    joined_date DATETIME NOT NULL,
    PRIMARY KEY (id))
    Engine = InnoDB DEFAULT CHARSET = utf8mb4;
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS mentoring_group(
    id VARCHAR(255) NOT NULL,
    group_type INT(11) NOT NULL,
    need_allow TINYINT(1) NOT NULL,
    openness TINYINT(1) NOT NULL,
    mentor VARCHAR(255) NOT NULL,
    created_date DATETIME NOT NULL,
    PRIMARY KEY (id))
    Engine = InnoDB DEFAULT CHARSET = utf8mb4;
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS email(
    id INT(11) NOT NULL,
    recipient_id VARCHAR(255) NOT NULL,
    date_sent DATETIME NOT NULL,
    frequency_level VARCHAR(255) NOT NULL,
    PRIMARY KEY (id))
    Engine = InnoDB DEFAULT CHARSET = utf8mb4;
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS tag(
    id INT(11) NOT NULL,
    name VARCHAR(255),
    PRIMARY KEY (id))
    Engine = InnoDB DEFAULT CHARSET = utf8mb4;
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS group_membership(
    group_id VARCHAR(255) NOT NULL,
    mentee_id VARCHAR(255) NOT NULL,
    group_joined_date DATETIME NOT NULL,
    PRIMARY KEY (group_id, mentee_id) )
    Engine = InnoDB DEFAULT CHARSET = utf8mb4;
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS email_question(
    email_id INT(11) NOT NULL,
    question_id VARCHAR(255) NOT NULL,
    PRIMARY KEY (email_id, question_id) )
    Engine = InnoDB DEFAULT CHARSET = utf8mb4;
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS tag_mentee(
    tag_id INT(11) NOT NULL,
    mentee_id VARCHAR(255) NOT NULL,
    PRIMARY KEY (tag_id, mentee_id) )
    Engine = InnoDB DEFAULT CHARSET = utf8mb4;
    """)

    cursor.execute("""
    CREATE TABLE tag_mentor(
    tag_id INT(11) NOT NULL,
    mentor_id VARCHAR(255) NOT NULL,
    PRIMARY KEY (tag_id, mentor_id))
    Engine = InnoDB DEFAULT CHARSET = utf8mb4;
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS tag_question(
    tag_id INT(11) NOT NULL,
    question_id VARCHAR(255) NOT NULL,
    PRIMARY KEY (tag_id, question_id))
    Engine = InnoDB DEFAULT CHARSET = utf8mb4;
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS comment(
    question_id VARCHAR(255) NOT NULL,
    comment_order INT(11) NOT NULL,
    comment_date DATETIME NOT NULL,
    body INT(11) NOT NULL,
    PRIMARY KEY (question_id, comment_order))
    Engine = InnoDB DEFAULT CHARSET = utf8mb4;
    """)

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS question(
    id VARCHAR(255) NOT NULL,
    mentee_id VARCHAR(255) NOT NULL,
    posted_date DATETIME NOT NULL,
    title VARCHAR(255) NOT NULL,
    body INT(11) NOT NULL,
    score INT(11) NOT NULL,
    PRIMARY KEY (id))
    Engine = InnoDB DEFAULT CHARSET = utf8mb4;
    ''')

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS mentor(
    id VARCHAR(255) NOT NULL,
    district VARCHAR(255),
    field VARCHAR(255),
    introduction LONGTEXT,
    joined_date DATETIME NOT NULL,
    PRIMARY KEY (id))
    Engine = InnoDB DEFAULT CHARSET = utf8mb4;
    """)


# Requirement3: insert data
def requirement3(host, user, password, directory):
    cnx = mysql.connector.connect(host=host, user=user, password=password, use_pure=True)
    cursor = cnx.cursor()
    cursor.execute('SET GLOBAL innodb_buffer_pool_size=2*1024*1024*1024;')
    cursor.execute("""USE DMA_project2_team03""")

    f_answer = open(directory + '\\answer.csv', 'r', encoding='utf-8')
    data_answer = f_answer.readlines()

    for i in range(1, len(data_answer)):
        if data_answer[i] == 'None':
            data_answer[i] = None
        li_answer = data_answer[i].replace('\n', '')
        li_answer = li_answer.split(',')
        cursor.execute('''INSERT INTO answer VALUES (%s,%s,%s,%s,%s,%s)''',
                       (li_answer[0], li_answer[1], li_answer[2], li_answer[3], li_answer[4], li_answer[5]))
        f_answer.close()

    f_comment = open(directory + '\\comment.csv', 'r', encoding='utf-8')
    data_comment = f_comment.readlines()

    for i in range(1, len(data_comment)):
        if data_comment[i] == 'None':
            data_comment[i] = None
        li_comment = data_comment[i].replace('\n', '')
        li_comment = li_comment.split(',')
        cursor.execute('''INSERT INTO comment VALUES (%s,%s,%s,%s)''',
                       (li_comment[0], li_comment[1], li_comment[2], li_comment[3]))
        f_comment.close()

    f_mentee = open(directory + '\\mentee.csv', 'r', encoding='utf-8')
    data_mentee = f_mentee.readlines()

    for i in range(1, len(data_mentee)):
        if data_mentee[i] == 'None':
            data_mentee[i] = None
        li_mentee = data_mentee[i].replace('\n', '')
        li_mentee = li_mentee.split(',')
        cursor.execute('''INSERT INTO mentee VALUES (%s,%s,%s)''', (li_mentee[0], li_mentee[1], li_mentee[2]))
        f_mentee.close()

    f_email = open(directory + '\\email.csv', 'r', encoding='utf-8')
    data_email = f_email.readlines()

    for i in range(1, len(data_email)):
        if data_email[i] == 'None':
            data_email[i] = None
        li_email = data_email[i].replace('\n', '')
        li_email = li_email.split(',')
        cursor.execute('''INSERT INTO email VALUES (%s, %s, %s,%s)''',
                       (li_email[0], li_email[1], li_email[2], li_email[3]))
        f_email.close()

    f_email_question = open(directory + '\\email_question.csv', 'r', encoding='utf-8')
    data_email_question = f_email_question.readlines()

    for i in range(1, len(data_email_question)):
        if data_email[i] == 'None':
            data_email[i] = None
        li_email_question = data_email_question[i].replace('\n', '')
        li_email_question = li_email_question.split(',')
        cursor.execute('''INSERT INTO email_question VALUES (%s, %s)''', (li_email_question[0], li_email_question[1]))
        f_email_question.close()

    f_group_membership = open(directory + '\\group_membership.csv', 'r', encoding='utf-8')
    data_group_membership = f_group_membership.readlines()

    for i in range(1, len(data_group_membership)):
        if data_group_membership[i] == 'None':
            data_group_membership = None
        li_group_membership = data_group_membership[i].replace('\n', '')
        li_group_membership = li_group_membership.split(',')
        cursor.execute('''INSERT INTO group_membership VALUES (%s, %s, %s)''',
                       (li_group_membership[0], li_group_membership[1], li_group_membership[2]))
        f_group_membership.close()

    f_mentoring_group = open(directory + '\\mentoring_group.csv', 'r', encoding='utf-8')
    data_mentoring_group = f_mentoring_group.readlines()

    for i in range(1, len(data_mentoring_group)):
        if data_mentoring_group[i] == 'None':
            data_mentoring_group[i] = None
        li_mentoring_group = data_mentoring_group[i].replace('\n', '')
        li_mentoring_group = li_mentoring_group.split(',')
        cursor.execute('''INSERT INTO mentoring_group VALUES (%s,%s,%s,%s,%s,%s)''', (
            li_mentoring_group[0], li_mentoring_group[1], li_mentoring_group[2], li_mentoring_group[3],
            li_mentoring_group[4], li_mentoring_group[5]))
        f_mentoring_group.close()

    f_tag_mentee = open(directory + '\\tag_mentee.csv', 'r', encoding='utf-8')
    data_tag_mentee = f_tag_mentee.readlines()

    for i in range(1, len(data_tag_mentee)):
        if data_tag_mentee[i] == 'None':
            data_tag_mentee[i] = None
        li_tag_mentee = data_tag_mentee[i].replace('\n', '')
        li_tag_mentee = li_tag_mentee.split(',')
        cursor.execute('''INSERT INTO tag_mentee VALUES (%s,%s)''', (li_tag_mentee[0], li_tag_mentee[1]))
        f_tag_mentee.close()

    f_tag_mentor = open(directory + '\\tag_mentor.csv', 'r', encoding='utf-8')
    data_tag_mentor = f_tag_mentor.readlines()

    for i in range(1, len(data_tag_mentor)):
        if data_tag_mentor == 'None':
            data_tag_mentor = None
        li_tag_mentor = data_tag_mentor[i].replace('\n', '')
        li_tag_mentor = li_tag_mentor.split(',')
        cursor.execute('''INSERT INTO tag_mentor VALUES (%s,%s)''', (li_tag_mentor[0], li_tag_mentor[1]))
        f_tag_mentor.close()

    f_tag_question = open(directory + '\\tag_question.csv', 'r', encoding='utf-8')
    data_tag_question = f_tag_question.readlines()

    for i in range(1, len(data_tag_question)):
        if data_tag_question == 'None':
            data_tag_question = None
        li_tag_question = data_tag_question[i].replace('\n', '')
        li_tag_question = li_tag_question.split(',')
        cursor.execute('''INSERT INTO tag_question VALUES (%s,%s)''', (li_tag_question[0], li_tag_question[1]))
        f_tag_question.close()

    f_tag = open(directory + '\\tag.csv', 'r', encoding='utf-8')
    data_tag = f_tag.readlines()

    for i in range(1, len(data_tag)):
        if data_tag[i] == 'None':
            data_tag[i] = None
        li_tag = data_tag[i].replace('\n', '')
        li_tag = li_tag.split(',')
        cursor.execute('''INSERT INTO tag (id, name) VALUES (%s,%s)''', (li_tag[0], li_tag[1]))
        f_tag.close()

    f_question = open(directory + '\\question.csv', 'r', encoding='utf-8')
    data_question = f_question.readlines()

    for i in range(1, len(data_question)):
        if data_question == 'None':
            data_question = None
        li_question = data_question[i].replace('\n', '')
        li_question = li_question.split(',')
        cursor.execute('INSERT INTO question VALUES (%s,%s,%s,%s,%s,%s)',
                       (li_question[0], li_question[1], li_question[2], li_question[3], li_question[4], li_question[5]))
        f_question.close()
    f_mentor = open(directory + '\\mentor.csv', 'r', encoding='utf-8')
    data_mentor = f_mentor.readlines()

    for i in range(1, len(data_mentor)):
        if data_mentor == 'None':
            data_mentor = None
        li_mentor = data_mentor[i].replace('\n', '')
        li_mentor = li_mentor.split(',')
        cursor.execute('INSERT INTO mentor VALUES (%s,%s,%s,%s,%s);',
                       (li_mentor[0], li_mentor[1], li_mentor[2], li_mentor[3], li_mentor[4]))
        f_mentor.close()

    cnx.commit()
    cursor.close()


def requirement4(host, user, password):
    cnx = mysql.connector.connect(host=host, user=user, password=password, use_pure=True)
    cursor = cnx.cursor()
    cursor.execute('SET GLOBAL innodb_buffer_pool_size=2*1024*1024*1024;')
    cursor.execute("""USE DMA_project2_team03""")

    cursor.execute('ALTER TABLE tag_mentee ADD CONSTRAINT FOREIGN KEY (tag_id) REFERENCES tag(id);')
    cursor.execute('ALTER TABLE tag_mentee ADD CONSTRAINT FOREIGN KEY (mentee_id) REFERENCES mentee(id);')
    cursor.execute('ALTER TABLE tag_question ADD CONSTRAINT FOREIGN KEY (tag_id) REFERENCES tag(id);')
    cursor.execute('ALTER TABLE tag_question ADD CONSTRAINT FOREIGN KEY (question_id) REFERENCES question(id);')
    cursor.execute('ALTER TABLE email_question ADD CONSTRAINT FOREIGN KEY (email_id) REFERENCES email(id);')
    cursor.execute('ALTER TABLE email_question ADD CONSTRAINT FOREIGN KEY (question_id) REFERENCES question(id);')
    cursor.execute('ALTER TABLE tag_mentor ADD CONSTRAINT FOREIGN KEY (tag_id) REFERENCES tag(id);')
    cursor.execute('ALTER TABLE tag_mentor ADD CONSTRAINT FOREIGN KEY (mentor_id) REFERENCES mentor(id);')
    cursor.execute('ALTER TABLE group_membership ADD CONSTRAINT FOREIGN KEY (group_id) REFERENCES mentoring_group(id);')
    cursor.execute('ALTER TABLE group_membership ADD CONSTRAINT FOREIGN KEY (mentee_id) REFERENCES mentee(id);')
    cursor.execute('ALTER TABLE comment ADD CONSTRAINT FOREIGN KEY (question_id) REFERENCES question(id);')
    cursor.execute('ALTER TABLE email ADD CONSTRAINT FOREIGN KEY (recipient_id) REFERENCES mentor(id);')
    cursor.execute('ALTER TABLE answer ADD CONSTRAINT FOREIGN KEY (mentor_id) REFERENCES mentor(id);')
    cursor.execute('ALTER TABLE answer ADD CONSTRAINT FOREIGN KEY (question_id) REFERENCES question(id);')
    cursor.execute('ALTER TABLE mentoring_group ADD CONSTRAINT FOREIGN KEY (mentor) REFERENCES mentor(id);')
    cursor.execute('ALTER TABLE question ADD CONSTRAINT FOREIGN KEY (mentee_id) REFERENCES mentee(id);')

    cnx.commit()
    cursor.close()


# TODO: WRITE CODE HERE


# TODO: WRITE CODE HERE


# TODO: REPLACE THE VALUES OF FOLLOWING VARIABLES'
host = 'localhost'
user = 'root'
password = 'dlgksdyd980831'
directory_in = 'C:\Program Files\dataset'

requirement1(host=host, user=user, password=password)
requirement2(host=host, user=user, password=password)
requirement3(host=host, user=user, password=password, directory=directory_in)
requirement4(host=host, user=user, password=password)