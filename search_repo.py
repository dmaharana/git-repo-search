import csv
import os
import subprocess
import threading

# Input parameters

# Input files
repos_file = 'input_files/repos.csv'
search_terms_file = 'input_files/search_terms.csv'

# Directory where repos will be cloned
workspace_dir = 'workspace'

# Directory where output files will be created
output_dir = 'output_files'

# Number of repos to be cloned simultaneously
clone_batch_size = 3

# File extensions to be considered in the result
# empty list will add all file types
# search_file_ext_list = []
search_file_ext_list = ['.py', '.txt']

# Git search flag
search_flag = 'iw'

# Git error code, 0 is success
GIT_ERROR_CODE = 128

# Final search result CSV header
search_result_csv_header = ['SEARCH_TERM','REPO_URL','REPO_BRANCH','MATCH_FILE']

# Final search result CSV file name
search_result_csv_file = f"{output_dir}/search_results.csv"

# OS command return sequence
cmd_out_header = ['CMD','CWD','RC','STDOUT']

###############################################################################
# read_file_content: Common function to read CSV file and return the content based on the 
#   required columns to be fetched
###############################################################################

def read_file_content(file_name, cols_list):
    content_list = []
    col_idx_list = []
    with open(file_name) as fh:
        csvr = csv.reader(fh)
        header = next(csvr, None)
        print(header)
        for c in cols_list:
            col_idx_list.append(cols_list.index(c))
        
        for row in csvr:
            nrow = []
            for i in col_idx_list:
                nrow.append(row[i])
            content_list.append(nrow)

    
    print(content_list)

    return content_list

###############################################################################
# run_os_cmd: Function to run OS command and return ['CMD','CWD','RC','STDOUT']
###############################################################################

def run_os_cmd(cmd_list, result=[None], index=0, cwd='.'):
    print('CMD={}'.format(cmd_list))
    output = subprocess.run(cmd_list, stdout=subprocess.PIPE, cwd=cwd)
    STDOUT = output.stdout.decode('utf-8')
    if STDOUT:
        print ('STDOUT={}'.format(STDOUT))
    RC = output.returncode
    print ('RC={}'.format(RC))

    result[index] = [' '.join(cmd_list), cwd, RC, STDOUT]

    return [' '.join(cmd_list), cwd, RC, STDOUT]

###############################################################################
# clone_repo_if_present_update: Function to clone a repo, if already exist then update with 'git pull'
###############################################################################

def clone_repo_if_present_update(cmd_list, result, index, cwd='.'):
    cmd, cwd, rc, stdout = run_os_cmd(cmd_list, result, index, cwd)

    if rc == GIT_ERROR_CODE:
        pull_cmd_list = ['git', 'pull']
        cmd, cwd, rc, stdout = run_os_cmd(pull_cmd_list, result, index, cwd)
    else:
        result[index] = [cmd, cwd, rc, stdout]
    
    return [cmd, cwd, rc, stdout]

###############################################################################
# clone_repos: Function to clone all repos parallely depending on the clone_batch_size
###############################################################################

def clone_repos(tasks, batch_size):
    all_results_list = []
    for i in range(0, len(tasks), batch_size):
        print(tasks[i:i+batch_size])
        batch_tasks = tasks[i:i+batch_size]
        thread_tasks = []
        results = [None] * len(batch_tasks)
        for i, t in enumerate(batch_tasks):
            td = threading.Thread(target=clone_repo_if_present_update, args=(t.split(), results, i, workspace_dir,))
            td.start()
            thread_tasks.append(td)

    
        for t in thread_tasks:
            t.join()
        
        all_results_list.extend(results)

    return all_results_list

###############################################################################
# search_repos: Function to search a term across all repos parallely
###############################################################################

def search_repos(tasks, batch_size, search_cmd):
    all_results_list = []
    for i in range(0, len(tasks), batch_size):
        print(tasks[i:i+batch_size])
        batch_tasks = tasks[i:i+batch_size]
        thread_tasks = []
        results = [None] * len(batch_tasks)
        for i, t in enumerate(batch_tasks):
            td = threading.Thread(target=run_os_cmd, args=(search_cmd.split(), results, i, t,))
            td.start()
            thread_tasks.append(td)

    
        for t in thread_tasks:
            t.join()

        all_results_list.extend(results)

        # print(f"all_results_list={all_results_list}")

    return all_results_list

###############################################################################
# get_dir_repo_url: Function to get git URL from the existing repo directory
#   this is required to map repo name and the directory name
###############################################################################

def get_dir_repo_url(repo_dirs_list):
    dir_repo_dict = {}

    thread_tasks = []
    results = [None] * len(repo_dirs_list)
    cmd = 'git remote get-url origin'
    for i, t in enumerate(repo_dirs_list):
        td = threading.Thread(target=run_os_cmd, args=(cmd.split(), results, i, t,))
        td.start()
        thread_tasks.append(td)


    for t in thread_tasks:
        t.join()

    print(f"results={results}")

    # cmd_out_header = ['CMD','CWD','RC','STDOUT']

    for row in results:
        rc = row[cmd_out_header.index('RC')]
        if rc == 0:
            repo_dir = row[cmd_out_header.index('CWD')]
            repo_url = row[cmd_out_header.index('STDOUT')]
            dir_repo_dict[repo_dir] = repo_url.replace('\n', '')
        else:
            print(f"Command error: {row[cmd_out_header.index('CMD')]}")

    return dir_repo_dict

###############################################################################
# get_repo_folders: Function to get list of all the directories in the input
#   directory
###############################################################################
def get_repo_folders(dirpath):
    contents = []

    for i in os.listdir(dirpath):
        # drop hidden
        if i[0] == '.':
            continue

        filepath = os.path.join(dirpath,i)
        is_dir = os.path.isdir(filepath)

        if is_dir:
            contents.append( filepath )
        else:
            continue
    
    return contents

###############################################################################
# write_to_csv: Function to write list of list to a CSV file
###############################################################################
def write_to_csv(output_list, out_filename):

    with open(out_filename, 'w', newline='') as fh:
        csvw = csv.writer(fh)
        csvw.writerows(output_list)
        print('Output file created: {}'.format(out_filename))

###############################################################################
# write_to_txt: Function to write text into a file
###############################################################################
def write_to_txt(text, out_filename):
    with open(out_filename, "w") as text_file:
        text_file.write("%s" % text)
        print('Output file created: {}'.format(out_filename))

###############################################################################
# allowed_ext: Function to check if allowed substrings are present 
#   in the input text
###############################################################################
def allowed_ext(fname, allowed_list):
    allowed_bool = False
    if allowed_list:
        for ex in allowed_list:
            if ex in fname:
                allowed_bool = True
                break
    else:
        allowed_bool = True

    return allowed_bool


###############################################################################
# collate_search_results: Function to collate search results which will 
#   eventually be written into a CSV and text files
###############################################################################
def collate_search_results(result_dict, dir_repo_dict, repo_branch_dict):
    s_results_list = []
    search_txt_log_dict = {}

    for k, v in result_dict.items():
        # print(f"SearchString={k}")
        print(f"{k}={v}")

        if k not in search_txt_log_dict:
            search_txt_log_dict[k] = []

        for row in v:
            rc = row[cmd_out_header.index('RC')]
            if rc == 0:
                repo_dir = row[cmd_out_header.index('CWD')]
                repo_url = dir_repo_dict[repo_dir]
                repo_branch = repo_branch_dict[repo_url]
                search_result = row[cmd_out_header.index('STDOUT')]

                search_txt_log_dict[k].append(f"REPO={repo_url};BRANCH={repo_branch}\n{search_result}")

                search_results_list = search_result.split('\n')
                for sr in search_results_list:
                    if 'HEAD:' in sr:
                        fname = sr.split(':')[-1]
                        add_to_list_bool = allowed_ext(fname, search_file_ext_list)

                        if add_to_list_bool:
                            entry = [k, repo_url, repo_branch, fname]
                            s_results_list.append(entry)
                
            else:
                print(f"Command error: {row[cmd_out_header.index('CMD')]}")

    return s_results_list, search_txt_log_dict

###############################################################################
# main: Function to initiate git repo clone, search and create output files
# Input files required:
#   - repository URL and branch
#   - search texts
# Output files created:
#   - Search results CSV
#   - One text file for each search term with the match results
###############################################################################
def main():
    clone_cmds_list = []
    search_cmds_list = []
    result_dict = {}
    repo_branch_dict = {}

    # Create workspace and output directories if not present
    run_os_cmd(['mkdir', '-p', workspace_dir])
    run_os_cmd(['mkdir', '-p', output_dir])
    
    # Get the repository URL and branch where search has to be done
    repo_cols_list = ['REPO_URL','REPO_BRANCH']
    repos_list = read_file_content(repos_file, repo_cols_list)

    for row in repos_list:
        repo_branch_dict[row[repo_cols_list.index('REPO_URL')]] = row[repo_cols_list.index('REPO_BRANCH')]

    # Get the list of search terms
    search_cols_list = ['SEARCH_TERM']
    sterms_list = read_file_content(search_terms_file, search_cols_list)

    # Clone the repositories into workspace
    #   if the repository already exist then perform a pull
    for row in repos_list:
        repo_url = row[repo_cols_list.index('REPO_URL')]
        repo_branch = row[repo_cols_list.index('REPO_BRANCH')]
        print ('CMD=git clone --mirror -b {} {}'.format(repo_branch, repo_url))
        clone_cmds_list.append('git clone --mirror -b {} {}'.format(repo_branch, repo_url))
    
    clone_repos(clone_cmds_list, clone_batch_size)

    # List the workspace directories
    repo_dirs = get_repo_folders(workspace_dir)
    dir_repo_dict = get_dir_repo_url(repo_dirs)
    
    # print(f"dir repo dict={dir_repo_dict}")
    # print(f"repo_dirs={repo_dirs}")

    # Perform search on repos
    for row in sterms_list:
        sterm = row[0]
        print ('CMD=git grep --break --heading --line-number -{} {} HEAD'.format(search_flag, sterm))
        search_cmds_list.append([sterm, f"git grep --break --heading --line-number -{search_flag} {sterm} HEAD"])

    for row in search_cmds_list:
        sterm = row[0]
        cmd = row[1]
        results = search_repos(repo_dirs, len(repo_dirs), cmd)
        result_dict[sterm] = results

    # Collate search results
    s_results_list, search_txt_log_dict = collate_search_results(result_dict, dir_repo_dict, repo_branch_dict)

    # Write search results to CSV 
    if len(s_results_list) > 0:
        s_results_list.insert(0, search_result_csv_header)
        write_to_csv(s_results_list, search_result_csv_file)

    # Write search content for each term to text file
    # print(f"search_txt_log_dict={search_txt_log_dict}")
    for k, v in search_txt_log_dict.items():
        if len(v) > 0:
            write_to_txt('\n'.join(v), f"{output_dir}/{k}.txt")



main()

