# datamanagement-couse
Project1
I connected python to mysql and added data. I created new tables based on the characteristics of given data and added them to mysql through preprocessing.

Project2
This is the project I did on Datamanagement lecture at SNU.
The data I used were all from a website called 'A'. This website encourages interaction between mentors and mentees. Users can use tags to identify their fields, questions and generally mentees follow famous mentors for more interaction. Mentors can answer questions that mentee asks and can also eastablish a new studying group based on people's location and majors. 
on part1(), I was required to make a new table called mentor and add mentor data to it through python.
Next, I used nested query to extract data that satisfy the requirements below.
- mentor's id, pro_mentor (whether the mentor is pro_mentor or not), age, have_introduction, have_field, num_of_answers, avg_of_answer_score, avg_of_answer_body, num_of_groups, avg_of_group_members, num_of_emails, num_of_tags 

Finally, I made a decision tree based on Node impurity method by calculating gini and entropy.

On part2(), I was required to do an association analysis of tags. I created a new view that includes
- tag_id, tag_name, num_mentor, num_mentee, num_question, score data

This view only includes top 50 data based on sorted scores. Then, I calculated the rating scores of the tags by using a new formula to create a new view called 'user-item-rating'. This view included information regarding
- user
- item (the name of the top-50 tag)
- rating

Next, I used 'user-item rating'view to create a new view called 'partial_user_item_rating' that has information of users who have more than 4 rating information.
Finally I have a new dataframe called partial_user_tag_rating that identifies either the user have rated the tag of not. This dataframe was used to make a frequent itemset and to implement association analysis.

On part3(), I made a recommendation system that recommends new tags to users. I used SVD, KNNBasic, KNNWithMeans algorithms and used cosine/pearson similarities calculation to get top-10 users and items. 
