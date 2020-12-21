import sys
from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('reddit relative scores').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
assert spark.version >= '2.3' # make sure we have Spark 2.3+

comments_schema = types.StructType([
    types.StructField('archived', types.BooleanType()),
    types.StructField('author', types.StringType()),
    types.StructField('author_flair_css_class', types.StringType()),
    types.StructField('author_flair_text', types.StringType()),
    types.StructField('body', types.StringType()),
    types.StructField('controversiality', types.LongType()),
    types.StructField('created_utc', types.StringType()),
    types.StructField('distinguished', types.StringType()),
    types.StructField('downs', types.LongType()),
    types.StructField('edited', types.StringType()),
    types.StructField('gilded', types.LongType()),
    types.StructField('id', types.StringType()),
    types.StructField('link_id', types.StringType()),
    types.StructField('name', types.StringType()),
    types.StructField('parent_id', types.StringType()),
    types.StructField('retrieved_on', types.LongType()),
    types.StructField('score', types.LongType()),
    types.StructField('score_hidden', types.BooleanType()),
    types.StructField('subreddit', types.StringType()),
    types.StructField('subreddit_id', types.StringType()),
    types.StructField('ups', types.LongType()),
    #types.StructField('year', types.IntegerType()),
    #types.StructField('month', types.IntegerType()),
])


def main(in_directory, out_directory):

    # read in the data
    comments = spark.read.json(in_directory, schema=comments_schema)

    # Cache the DF since we will be using it later
    comments = comments.cache()

    # Calculate average scores for each subreddit
    sub_averages_grouped = comments.groupBy('subreddit').mean('score')

    # Filter any averages that are less than 0 
    sub_averages_grouped = sub_averages_grouped.filter(sub_averages_grouped['avg(score)'] > 0)

    # Join the average score to the collection of all comments
    comments_with_avg_score = comments.join(sub_averages_grouped, ['subreddit'])

    # Divide to get the relative score
    comments_with_rel_score = comments_with_avg_score.withColumn('rel_score', comments_with_avg_score['score'] / comments_with_avg_score['avg(score)'])

    # Cache it since we will be using it later
    comments_with_rel_score = comments_with_rel_score.cache()

    # Determine the max relative score for each subreddit
    max_rel_score_grouped = comments_with_rel_score.groupBy('subreddit').agg(functions.max('rel_score')).select('subreddit', 'max(rel_score)')

    # Join again to get the best comment on each subreddit: we need this step to get the author
    max_rel_score_joined = comments_with_rel_score.join(max_rel_score_grouped, ['subreddit'])
    output_df = max_rel_score_joined.filter(max_rel_score_joined['rel_score'] == max_rel_score_joined['max(rel_score)']).select('subreddit', 'author', 'rel_score')

    # Output to json
    output_df.write.json(out_directory, mode='overwrite')


if __name__=='__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    main(in_directory, out_directory)







