import tarfile, sys, io, json, os, tempfile, subprocess, yaml, pickle
from pathlib import Path
from pprint import pprint
import requests
from dotenv import load_dotenv

load_dotenv()

GRAF_AUTH = os.getenv("GRAFANA_AUTH")

def get_free_filename(stub, directory, suffix=''):
    """ Get a file name that is unique in the given directory
    input: the "stub" (string you would like to be the beginning of the file
        name), the name of the directory, and the suffix (denoting the file type)
    output: file name using the stub and suffix that is currently unused
        in the given directory
    """
    counter = 0
    while True:
        file_candidate = '{}{}-{}{}'.format(
            str(directory), stub, counter, suffix)
        if Path(file_candidate).exists():
            counter += 1
        else:  # No match found
            print("get_free_filename no suffix")
            Path(file_candidate).touch()
            return file_candidate

def run():
    # path for changeset using RHODS environment
    homed = "/pipelines/component/"
    
    src = homed+"src/"
    if not Path(src).exists():
        Path(src).mkdir()
        # os.chmod(src, 777)
        
    cwd = homed+"cwd/"
    if not Path(cwd).exists():
        Path(cwd).mkdir()
        # os.chmod(cwd, 777)

    # # LOKI_TOKEN=$(oc whoami -t)
    # # curl -H "Authorization: Bearer $LOKI_TOKEN" "https://grafana-open-cluster-management-observability.apps.nerc-ocp-infra.rc.fas.harvard.edu/api/datasources/proxy/1/api/v1/query" --data-urlencode 'query=kube_pod_container_info{namespace="ai4cloudops-11855c"}' | jq

    grafana_addr = 'https://grafana.apps.obs.nerc.mghpcc.org/api/datasources/proxy/1/api/v1/query'

    headers={
        'Authorization': GRAF_AUTH,
        'Content-Type': 'application/x-www-form-urlencoded'
        }

    name_space = "ai4cloudops-11855c"
    params = {
        "query": "kube_pod_container_info{namespace='"+name_space+"'}"
        }

    kube_pod_container_info = requests.get(grafana_addr, params=params, headers=headers)
    
    containers: set[tuple[str, str]] = set()
    
    try:
        for info in kube_pod_container_info.json()['data']['result']:
            im = "/".join(info['metric']['image'].split("/")[1:])
            con = info['metric']['container']
            
            # skip if part of RHODS pipeline
            if im.startswith('rhoai'):
                continue
            
            containers.add((im, con))
            
    except Exception as e:
        print("Results were: ", kube_pod_container_info)
        print(e)

    # print for logs
    for im, con in containers:
        print(f"{im}: {con}")
    
    # Should be something like this command: image_names = "/."join(kube_pod_container_info.json()['data']['result'][:]['metric']['image'].split("/")[1:])
    # image_name = ["rohankumar1/introspect1:0"] #, "rohankumar1/introspect2:0", "rohankumar1/introspect3:0"]
    # container name should be also from k8s info
    
    image_layer_dirs = []

    # this command pulls the image, and downloads it to the appropriate folder (in this case, cwd/introspected_container)
    for image, name in containers:
        cmd1 = f"bash {src}download-frozen-image-v2.sh {cwd}{name} {image}"
        p_cmd1 = subprocess.Popen(cmd1.split(" "), stdin=subprocess.PIPE)
        p_cmd1.communicate()
        image_layer_dirs.append(cwd+name)
    
    # this will hold final value to return
    changesets_dict = {}
    
    # build changesets for each container
    for i, (_, name) in enumerate(containers):
        try:
            image_d = {}
            image_meta_d = {}
            with open(cwd+f"logfile_reading_tar_{name}.log", "w") as log_file:
                for root, subdirs, files in os.walk(image_layer_dirs[i]):
                    for file_name in files:
                        print(os.path.join(root, file_name))
                        # print(file_name)
                        if file_name == "manifest.json":
                            # json_file = tar.extractfile(member)
                            with open(os.path.join(root, file_name), "r") as json_file:
                                content = json.load(json_file)
                                image_meta_d[file_name] = content
                                pprint(file_name, log_file)
                                pprint(content, log_file)
                                pprint("\n", log_file)
                        elif file_name == "json":
                            # json_file = tar.extractfile(member)
                            with open(os.path.join(root, file_name), "r") as json_file:
                                content = json.load(json_file)
                                image_meta_d[root] = content
                                pprint(root, log_file)
                                pprint(content, log_file)
                                pprint("\n", log_file)
                        elif file_name[-4:] == "json":
                            # json_file = tar.extractfile(member)
                            with open(os.path.join(root, file_name), "r") as json_file:
                                content = json.load(json_file)
                                image_meta_d[file_name] = content
                                pprint(file_name, log_file)
                                pprint(content, log_file)
                                pprint("\n", log_file)
                        elif file_name[-3:] == "tar":
                            # tar_bytes = io.BytesIO(tar.extractfile(member).read())
                            tar_file = os.path.join(root, file_name)
                            inner_tar = tarfile.open(tar_file)
                            image_d[root.split("/")[-1]] = inner_tar.getnames()
                            pprint(tar_file, log_file)
                            pprint(inner_tar.getnames(), log_file)
                            pprint("\n", log_file)
                            inner_tar.close()

            # build changesets
            changesets_l = []             
            changesets_dir = cwd+"changesets/"
            if not Path(changesets_dir).exists():
                Path(changesets_dir).mkdir()
                
            with open(cwd+f"logfile_changeset_gen_{name}.log", "w") as log_file:
                for image_manifest in image_meta_d["manifest.json"]:
                    image_config_name = image_manifest["Config"]
                    image_config_history_iter = iter(image_meta_d[image_config_name]["history"])
                    
                    for layer in image_manifest["Layers"]:
                        try:
                            image_config_history = next(image_config_history_iter)
                            while 'empty_layer' in image_config_history:
                                print(image_config_history, "skipped")
                                image_config_history = next(image_config_history_iter)
                        except StopIteration:
                            print("image_config_history is None")
                            sys.exit(-1)
                        print(image_config_history)
                        pprint(layer, log_file)
                        pprint(image_config_history['created_by'], log_file)
                        pprint(image_config_history['created'], log_file)
                        pprint(image_d[layer.split("/")[0]], log_file)

                        # yaml_in = {'open_time': open_time, 'close_time': close_time, 'label': label, 'changes': changes}
                        yaml_in = {'labels': ['unknown'], 'changes': image_d[layer.split("/")[0]]}
                        changeset_filename = get_free_filename("unknown", changesets_dir, ".yaml")
                        with open(changeset_filename, 'w') as outfile:
                            print("gen_changeset", os.path.dirname(outfile.name))
                            print("gen_changeset", changeset_filename)
                            yaml.dump(yaml_in, outfile, default_flow_style=False)
                        changesets_l.append(yaml_in)
                        
                changesets_dict[name] = changesets_l
        except:
            pass
                
    return changesets_dict, kube_pod_container_info

if __name__ == "__main__":
    # run system, output is the complete changesets
    changesets_l = run()
    
    # path to place changeset
    # cs_dump_path = "/home/cc/Praxi-study/Praxi-Pipeline/get_layer_changes/cwd/changesets_l_dump"
    
    # # put changeset into a specific area
    # with open(cs_dump_path, 'wb') as writer:
    #     pickle.dump(changesets_l, writer)
