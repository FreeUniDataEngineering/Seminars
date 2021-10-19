from hdfs import InsecureClient

'''
    DOC:
        https://hdfscli.readthedocs.io/en/latest/api.html#hdfs.client.Client.download
        
    NOTE:
        These commands must be run inside the container named ‘edge’ (running on port 7777)
        - Open container CLI
        - run `/bin/bash`
        - run `python` to enter Python console and work with HDFSCli api 
    
    REQUIREMENTS: 
        - Docker running containers from Hadoop_Hive image
            https://github.com/FreeUniDataEngineering/hadoop_hive
        - run `pip install hdfs` (run inside 'edge' container)
'''

root_dir = '/'
data_lake_path = root_dir + 'data_lake'

# Set-up a Client without authorization
client = InsecureClient('http://namenode:50070', root=root_dir)

print('List dirs and files in root directory with detailed metadata (status=True)')
print(client.list(root_dir, status=True))


def get_sub_dirs(dir_path, full_path=False):
    """ List subdirectories for specified directory """
    all_files = client.list(dir_path, status=True)
    dirs = [e[0] for e in all_files if e[1]['type'] == 'DIRECTORY']
    return dirs if not full_path or dir_path == '/' else [dir_path + '/' + d for d in dirs]


def rec_list(paths=[]):
    """ Recursively list sub-directories """
    if paths:
        dir = paths[0]
        print(dir)
        sub_dirs = get_sub_dirs(dir, full_path=True)
        rec_list([*sub_dirs, *paths[1:]])


print('Recursively listing all files under root: ')
rec_list([root_dir])


print('Depth-first walk of filesystem: ')
dirs = client.walk(data_lake_path, depth=0)
for e in dirs:
    print(e)


print('Listing metadata for root dir: ')
print(client.content(root_dir))


print('Creating a directory: ')
new_dir_name = 'just_been_created'
client.makedirs(f'{data_lake_path}/{new_dir_name}')
print(client.list(data_lake_path))


print('Renaming a directory')
updated_name = 'i_have_been_renamed'
client.rename(f'{data_lake_path}/{new_dir_name}', f'{data_lake_path}/{updated_name}')
print(client.list(data_lake_path))


print('Deleting a file or directory')
client.delete(f'{data_lake_path}/{updated_name}', recursive=True, skip_trash=True)
print(client.list(data_lake_path))


print('Uploading a file')
path_to_local_file = ''  # excluding file name
file_name = 'some_data.txt'
client.upload(f'{data_lake_path}/{file_name}', local_path=f'{path_to_local_file}/{file_name}')
print(client.list(data_lake_path))


print('Reading a file as bytes and decoding to utf-8: ')
with client.read(f'{data_lake_path}/{file_name}', encoding='utf-8') as reader:
    file = reader.read()
    print(file)


print('Writing to a new file: ')
dest_path = f'{data_lake_path}/new_sub_directory'
file_name = 'say_hello.txt'
data = 'Welcome to Hadoop Ecosystem!'
client.write(f'{dest_path}/{file_name}', data=data, overwrite=True, append=False, encoding='utf-8')
print(client.list(dest_path))


print('Downloading a file: ')
client.download(f'{dest_path}/{file_name}', local_path='/', overwrite=True)



