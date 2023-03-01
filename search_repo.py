import csv
import os
import subprocess
import threading

repos_file = 'input_files/repos.csv'
search_terms_file = 'input_files/search_terms.csv'
workspace_dir = 'workspace'
output_dir = 'output_files'
clone_batch_size = 3

search_result_csv_header = ['SEARCH_TERM','REPO_URL','REPO_BRANCH','MATCH_FILE']
search_result_csv_file = f"{output_dir}/search_results.csv"

cmd_out_header = ['CMD','CWD','RC','STDOUT']

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

def clone_repos(tasks, batch_size):
    all_results_list = []
    for i in range(0, len(tasks), batch_size):
        print(tasks[i:i+batch_size])
        batch_tasks = tasks[i:i+batch_size]
        thread_tasks = []
        results = [None] * len(batch_tasks)
        for i, t in enumerate(batch_tasks):
            td = threading.Thread(target=run_os_cmd, args=(t.split(), results, i, workspace_dir,))
            td.start()
            thread_tasks.append(td)

    
        for t in thread_tasks:
            t.join()
        
        all_results_list.extend(results)

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

def write_to_csv(output_list, out_filename):

    with open(out_filename, 'w', newline='') as fh:
        csvw = csv.writer(fh)
        csvw.writerows(output_list)
        print('Output file created: {}'.format(out_filename))

def write_to_txt(text, out_filename):
    with open(out_filename, "w") as text_file:
        text_file.write("%s" % text)
        print('Output file created: {}'.format(out_filename))

def main():
    clone_cmds_list = []
    search_cmds_list = []
    result_dict = {}
    repo_branch_dict = {}
    search_txt_log_dict = {}

    repo_cols_list = ['REPO_URL','REPO_BRANCH']
    repos_list = read_file_content(repos_file, repo_cols_list)

    run_os_cmd(['mkdir', '-p', output_dir])
    
    for row in repos_list:
        repo_branch_dict[row[repo_cols_list.index('REPO_URL')]] = row[repo_cols_list.index('REPO_BRANCH')]

    search_cols_list = ['SEARCH_TERM']
    sterms_list = read_file_content(search_terms_file, search_cols_list)

    if os.path.isdir(workspace_dir) == False:
        run_os_cmd(['mkdir', '-p', workspace_dir])

        for row in repos_list:
            repo_url = row[repo_cols_list.index('REPO_URL')]
            repo_branch = row[repo_cols_list.index('REPO_BRANCH')]
            print ('CMD=git clone --mirror -b {} {}'.format(repo_branch, repo_url))
            clone_cmds_list.append('git clone --mirror -b {} {}'.format(repo_branch, repo_url))
        
        clone_repos(clone_cmds_list, clone_batch_size)

    repo_dirs = get_repo_folders(workspace_dir)
    dir_repo_dict = get_dir_repo_url(repo_dirs)
    
    # print(f"dir repo dict={dir_repo_dict}")

    # print(f"repo_dirs={repo_dirs}")

    for row in sterms_list:
        sterm = row[0]
        print ('CMD=git grep --break --heading --line-number -iw {} HEAD'.format(sterm))
        search_cmds_list.append([sterm, f"git grep --break --heading --line-number -iw {sterm} HEAD"])

    for row in search_cmds_list:
        sterm = row[0]
        cmd = row[1]
        results = search_repos(repo_dirs, len(repo_dirs), cmd)
        result_dict[sterm] = results

    s_results_list = []
    for k, v in result_dict.items():
        # print(f"SearchString={k}")
        print(f"{k}={v}")
        mfiles_list = []

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
                    if 'HEAD' in sr:
                        f = sr.split(':')[-1]
                        e = [k, repo_url, repo_branch, f]
                        s_results_list.append(e)
                
            else:
                print(f"Command error: {row[cmd_out_header.index('CMD')]}")
            
    if len(s_results_list) > 0:
        s_results_list.insert(0, search_result_csv_header)
        write_to_csv(s_results_list, search_result_csv_file)

    print(f"search_txt_log_dict={search_txt_log_dict}")

    for k, v in search_txt_log_dict.items():
        if len(v) > 0:
            write_to_txt('\n'.join(v), f"{output_dir}/{k}.txt")



main()

