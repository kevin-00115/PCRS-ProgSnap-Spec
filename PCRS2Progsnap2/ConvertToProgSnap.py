import pandas as pd
from datetime import datetime
from os import mkdir

if __name__ == '__main__':
    fdir = '.'

    # read relevant files
    df_problem = pd.read_csv('{}/problems_python_problem.csv'.format(fdir))
    df_submission = pd.read_csv('{}/problems_python_submission.csv'.format(fdir))
    df_testrun = pd.read_csv('{}/problems_python_testrun.csv'.format(fdir))
    df_challenge = pd.read_csv('{}/content_challenge.csv'.format(fdir))

    # generate codestate table with unique codestate ids
    df_codestate = df_submission['submission'].drop_duplicates().reset_index(drop=True).reset_index()[['index', 'submission']]

    # generate the resource files and link table
    try:
        mkdir('codestates')
    except FileExistsError:
        pass
    submission_files = []
    for index, row in df_codestate.iterrows():
        path = 'codestates/codestate_{}.txt'.format(row['index'])
        submission_files.append(path)
        with open(path, 'wb') as fp:
            fp.write(row['submission'].encode('utf-8'))
    df_codestate_link = df_codestate.copy()
    df_codestate_link['filepath'] = submission_files
    df_codestate_link = df_codestate_link[['index', 'filepath']]
    df_codestate_link.columns = ['id', 'filepath']
    df_codestate_link.to_csv('codestate_link_table.csv')

    # finish creating the codestate table
    df_codestate = df_codestate.merge(df_submission[['id', 'submission']], how='right', on='submission')
    df_codestate = df_codestate[['id', 'index']]
    df_codestate.columns = ['submission_id', 'CodeStateID']
    
    # process submission events
    df_submission = df_submission.merge(df_codestate, how='left', left_on='id', right_on='submission_id')
    df_submission = df_submission.drop(['submission', 'score', 'has_best_score', 'submission_id', 'pyta'], axis=1)
    df_submission['ParentEventID'] = None
    df_submission['TestID'] = None
    df_submission['EventType'] = 'Submit'
    # convert timestamp from string to python format
    df_submission['Attempt'] = df_submission.groupby(['problem_id', 'user_id'])['timestamp'].apply(lambda x: x.rank(method='min'))

    # process test run events
    df_testrun = df_testrun.merge(df_submission, how='left', on=None, left_on='submission_id', right_on='id')
    df_testrun = df_testrun.merge(df_codestate, how='left', on='submission_id')
    df_testrun = df_testrun.drop(['test_passed', 'id_y', 'CodeStateID_y', 'ParentEventID', 'TestID', 'EventType'], axis=1)
    df_testrun.columns = ['EventID', 'ParentEventID', 'TestID', 'SubjectID', 'CourseSectionID', 'ServerTimestamp',
                        'ProblemID', 'CodeStateID', 'Attempt']
    df_testrun['EventType'] = 'Run.Test'

    # match submission and test run column names
    df_submission.columns = ['EventID', 'SubjectID', 'CourseSectionID', 'ServerTimestamp', 'ProblemID', 'CodeStateID', 'ParentEventID', 'TestID', 'EventType', 'Attempt']
    
    # concatenate into main table
    df = pd.concat([df_testrun, df_submission], sort=False)
    df['ToolInstances'] = 'PCRS'
    df['ServerTimezone'] = 'America/Toronto'

    # save the problem descriptions to text files and generate the resource tables
    try:
        mkdir('problemdescriptions')
    except FileExistsError:
        pass
    problem_files = []
    for index, row in df_problem.iterrows():
        path = 'problemdescriptions/problem_description_{}.txt'.format(row['id'])
        problem_files.append(path)
        with open(path, 'wb') as fp:
            fp.write(str(row['description']).encode('utf-8'))
    df_problem_description_link = df_problem[['id', 'description']].copy()
    df_problem_description_link['filepath'] = problem_files
    df_problem_description_link = df_problem_description_link[['id', 'filepath']]
    df_problem_description_link.to_csv('problem_description_link_table.csv')

    # create the resource table for the starter code
    try:
        mkdir('startercode')
    except FileExistsError:
        pass
    starter_code_files = []
    for index, row in df_problem.iterrows():
        path = 'startercode/starter_code_{}.txt'.format(row['id'])
        starter_code_files.append(path)
        with open(path, 'wb') as fp:
            fp.write(str(row['starter_code']).encode('utf-8'))
    df_starter_code_link = df_problem[['id', 'starter_code']].copy()
    df_starter_code_link['filepath'] = starter_code_files
    df_starter_code_link = df_starter_code_link[['id', 'filepath']]
    df_starter_code_link.to_csv('starter_code_link_table.csv')

    # create the resource table for the solutions
    try:
        mkdir('solutions')
    except FileExistsError:
        pass
    solution_files = []
    for index, row in df_problem.iterrows():
        path = 'solutions/solution_{}.txt'.format(row['id'])
        solution_files.append(path)
        with open(path, 'wb') as fp:
            fp.write(str(row['solution']).encode('utf-8'))
    df_solution_link = df_problem[['id', 'solution']].copy()
    df_solution_link['filepath'] = solution_files
    df_solution_link = df_solution_link[['id', 'filepath']]
    df_solution_link.to_csv('solution_link_table.csv')

    # merge in problem table to get assignment ids
    df = df.merge(df_problem[['id', 'challenge_id']], how='left', left_on='ProblemID', right_on='id')
    df.columns = list(df.columns)[:-1] + ['AssignmentID']

    # merge in challenge table to get isGraded
    df = df.merge(df_challenge[['id', 'is_graded']], how='left', left_on='AssignmentID', right_on='id')
    df = df.drop(['id_x', 'id_y'], axis=1)
    df.columns = list(df.columns)[:-1] + ['AssignmentIsGraded']

    # I'm assuming that a problem is graded if and only if the relevant assignment is graded
    df['ProblemIsGraded'] = df['AssignmentIsGraded']

    # reorder columns
    df = df[['EventID', 'ParentEventID', 'EventType', 'SubjectID', 'ToolInstances', 'CourseSectionID', 'CodeStateID', 'ServerTimestamp', 'ServerTimezone', 'AssignmentID', 'AssignmentIsGraded', 'ProblemID', 'ProblemIsGraded', 'Attempt', 'TestID']]

    df.to_csv('{}/pcrs_main_table_draft.csv'.format(fdir), index=False)