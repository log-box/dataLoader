import csv

import psycopg2


def csv_reader(file_obj):
    """
    Read a csv file
    """
    _list = []
    i = 1
    reader = csv.reader(file_obj, skipinitialspace=True)
    con = psycopg2.connect(
        database="dataloader",
        user="postgres",
        password="1",
        host="127.0.0.1",
        port="5432"
    )
    if con:
        print('connected')
    for string in reader:
        if string[0] != '#':
            cur = con.cursor()
            question = string[1]
            rightAnswer = string[3]
            commentForJudge = string[4]
            aboutQuestion = string[5]
            link = string[6]
            wrongAnswerOne = string[7]
            wrongAnswerTwo = string[8]
            wrongAnswerThree = string[9]
            themeOfQuestion = string[10]
            questionCategory = string[11]
            sectionOfQuestion = string[12]
            complexityOfQuestion = string[14]
            user = string[16]
            approve = 1
            date_ques_sub = string[17]
            base_date = string[20]
            sqlData = 'INSERT INTO "dataLoader_questions" (question,"rightAnswer","commentForJudge","aboutQuestion",' \
                      'link,"wrongAnswerOne","wrongAnswerTwo","wrongAnswerThree","themeOfQuestion","questionCategory"' \
                      ',"sectionOfQuestion","complexityOfQuestion","user",approve,date_ques_sub,base_date) ' \
                      'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'
            data = (
            question, rightAnswer, commentForJudge, aboutQuestion, link, wrongAnswerOne, wrongAnswerTwo,
            wrongAnswerThree, themeOfQuestion, questionCategory, sectionOfQuestion, complexityOfQuestion, user, approve,
            date_ques_sub, base_date)
            cur.execute(sqlData, data)
            print(f'Строка№ {i} записана успешно')
            i += 1
            con.commit()
    con.close()


if __name__ == "__main__":
    csv_path = "data.csv"
    with open(csv_path, "r", encoding='utf-8', newline='') as f_obj:
        csv_reader(f_obj)

