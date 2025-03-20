
from typing import NamedTuple

import os
import kfp, kfp_tekton, kubernetes
import kfp.dsl as dsl
from kfp.components import InputPath, InputTextFile, OutputPath, OutputTextFile
from kfp.components import func_to_container_op
import kubernetes.client
from dotenv import load_dotenv

os.environ["DEFAULT_STORAGE_CLASS"] = "ocs-external-storagecluster-ceph-rbd"
os.environ["DEFAULT_ACCESSMODES"] = "ReadWriteOnce"

AWS_ACCESS = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY")
GRAF_AUTH = os.getenv("GRAFANA_AUTH")
BEARER_TOK = os.getenv("BEARER_TOKEN")


kubeflow_endpoint="https://ds-pipeline-pipelines-definition-ai4cloudops-11855c.apps.shift.nerc.mghpcc.org"


def load_model(clf_path: OutputPath(str), index_tag_mapping_path: OutputPath(str), tag_index_mapping_path: OutputPath(str), index_label_mapping_path: OutputPath(str), label_index_mapping_path: OutputPath(str)):
    ''' Loads the vw model file and Hybrid class object '''
    import boto3
    import os
    
    # getting model from AWS s3
    s3 = boto3.resource(service_name='s3', 
                        region_name='us-east-1', 
                        aws_access_key_id=AWS_ACCESS, 
                        aws_secret_access_key=AWS_SECRET)
    
    model_localpath = '/pipelines/component/src/model.json'
    index_tag_mapping_localpath = '/pipelines/component/src/index_tag_mapping'
    tag_index_mapping_localpath = '/pipelines/component/src/tag_index_mapping'
    index_label_mapping_localpath = '/pipelines/component/src/index_label_mapping'
    label_index_mapping_localpath = '/pipelines/component/src/label_index_mapping'

    s3.Bucket('praxi-model-xgb-02').download_file(Key='model.json', Filename=model_localpath)
    os.popen('cp {0} {1}'.format(model_localpath, clf_path))
    s3.Bucket('praxi-model-xgb-02').download_file(Key='index_tag_mapping', Filename=index_tag_mapping_localpath)
    os.popen('cp {0} {1}'.format(index_tag_mapping_localpath, index_tag_mapping_path))
    s3.Bucket('praxi-model-xgb-02').download_file(Key='tag_index_mapping', Filename=tag_index_mapping_localpath)
    os.popen('cp {0} {1}'.format(tag_index_mapping_localpath, tag_index_mapping_path))
    s3.Bucket('praxi-model-xgb-02').download_file(Key='index_label_mapping', Filename=index_label_mapping_localpath)
    os.popen('cp {0} {1}'.format(index_label_mapping_localpath, index_label_mapping_path))
    s3.Bucket('praxi-model-xgb-02').download_file(Key='label_index_mapping', Filename=label_index_mapping_localpath)
    os.popen('cp {0} {1}'.format(label_index_mapping_localpath, label_index_mapping_path))

generate_loadmod_op = kfp.components.create_component_from_func(load_model, output_component_file=None, base_image="zongshun96/load_model_s3:0.01")

def generate_changesets(user_in: str, cs_path: OutputPath(str), args_path: OutputPath(str)):
    '''generate changeset from OpenShift'''
    import read_layered_image
    import pickle
    
    # changesets_dict should be dictionary containing a changeset for each container, key is name
    changesets_dict, info_dict = read_layered_image.run()
    
    print("INFO_DICT:", info_dict)
    
    with open(cs_path, 'wb') as writer:
        pickle.dump(changesets_dict, writer)
    with open(args_path, 'wb') as argfile:
        pickle.dump(user_in, argfile)
        
generate_changeset_op = kfp.components.create_component_from_func(generate_changesets, output_component_file=None, base_image="rohankumar1/prom-get-layers:latest")



def generate_tagset(input_args_path: InputPath(str), changeset_path: InputPath(str), output_text_path: OutputPath(str), output_args_path: OutputPath(str)):
    '''generate tagset from the changeset'''
    from columbus.columbus import columbus
    import pickle
    
    # Load data from previous component
    with open(input_args_path, 'rb') as in_argfile:
        user_in = pickle.load(in_argfile)
    with open(changeset_path, 'rb') as in_changesets_dict:
        changesets_dict = pickle.load(in_changesets_dict)

    # just checking...
    assert type(changesets_dict)==dict
                              
    # Tagset Generator FOR EACH CONTAINER
    tagsets_dict = {}
    for name in changesets_dict.keys():
        tagsets_l = []
        for changeset in changesets_dict[name]:
            # tags = tagset_gen.get_columbus_tags(changeset['changes'])
            tag_dict = columbus(changeset['changes'], freq_threshold=2)
            tags = ['{}:{}'.format(tag, freq) for tag, freq in tag_dict.items()]
            cur_dict = {'labels': changeset['labels'], 'tags': tags}
            tagsets_l.append(cur_dict)
            
        # add tagset to dictionary
        tagsets_dict[name] = tagsets_l

    # dump tagsets into PV
    with open(output_text_path, 'wb') as writer:
        pickle.dump(tagsets_dict, writer)
    with open(output_args_path, 'wb') as argfile:
        pickle.dump(user_in, argfile)
        
generate_tagset_op = kfp.components.create_component_from_func(generate_tagset, output_component_file=None, base_image="zongshun96/taggen_openshift:0.01")



def gen_prediction(clf_path: InputPath(str), index_tag_mapping_path: InputPath(str), tag_index_mapping_path: InputPath(str), index_label_mapping_path: InputPath(str), label_index_mapping_path: InputPath(str), test_tags_path: InputPath(str), prediction_path: OutputPath(str)):
    '''generate prediction given model'''
    import yaml
    import pickle
    import time
    import tagsets_XGBoost
    import xgboost as xgb
    import boto3
    import requests
    import pandas as pd
    from pymongo import MongoClient
    import certifi  
    
    # cwd for OpenShift
    cwd = "/pipelines/component/cwd/"

    # load from previous component
    with open(test_tags_path, 'rb') as reader:
        tagsets_dict = pickle.load(reader)
    
    assert type(tagsets_dict) is dict
    
    # ############
    # Iterating through each container
    # ############
    # r = redis.Redis(host='redis', port=6379)
    client = MongoClient("mongodb+srv://jli3469:kvLImlt6UhjzZpu3@praxium-logs.lvy0r.mongodb.net/?retryWrites=true&w=majority&appName=praxium-logs", tlsCAFile=certifi.where())
    db = client["praxium-logs"]
    mongo = db["deathstarbench-praxipaas-test"]
    
    # get timestamp
    grafana_addr = 'https://grafana.apps.obs.nerc.mghpcc.org/api/datasources/proxy/1/api/v1/query'
    headers={
        'Authorization': GRAF_AUTH,
        'Content-Type': 'application/x-www-form-urlencoded'
        }

    cpu_usage_params = {
        "query": "kube_pod_container_info{namespace=\"ai4cloudops-11855c\"}[3h:1m]"
        }
    
    m_cpu_use = requests.get(grafana_addr, params=cpu_usage_params, headers=headers)
    
    # metric data
    metrics_d = dict()
    for i in m_cpu_use.json()['data']['result']:
        # add current running containers into metrics_d
        cname = i['metric']['container']
        
        # create the temporary dataframe to hold metric data
        temp = pd.DataFrame(i['values'], columns=['Time', 'CONTAINER_INFO']).astype(float).round(4)
        temp.Time = pd.to_datetime(temp.Time, unit='s')
        temp.set_index('Time', inplace=True)
        
        # check to see if timestamp is more recent than current timestamp for this container
        if cname not in metrics_d or metrics_d[cname]['df'].index[0] < temp.index[0]:
            # just add to dict
            metrics_d[cname] = {
                'df': temp,
                'container_id': i['metric']['container_id']
            }
    
    print(f"Containers to predict: {tagsets_dict.keys()}")
        
    for cname, tagset in tagsets_dict.values():

        # get feature matrix from tagset
        _, feature_matrix, _ = tagsets_XGBoost.tagsets_to_matrix(tagsets=tagset, index_tag_mapping_path=index_tag_mapping_path, tag_index_mapping_path=tag_index_mapping_path, index_label_mapping_path=index_label_mapping_path, label_index_mapping_path=label_index_mapping_path, train_flag=False, inference_flag=True, cwd=cwd)
        
        # prediction
        BOW_XGB = xgb.XGBClassifier(max_depth=10, learning_rate=0.1,silent=False, objective='binary:logistic', \
                        booster='gbtree', n_jobs=8, nthread=None, gamma=0, min_child_weight=1, max_delta_step=0, \
                        subsample=0.8, colsample_bytree=0.8, colsample_bylevel=0.8, reg_alpha=0, reg_lambda=1)
        BOW_XGB.load_model(clf_path)
        pred_label_matrix = BOW_XGB.predict(feature_matrix)
        results = tagsets_XGBoost.one_hot_to_names(index_label_mapping_path, pred_label_matrix)
        
        # create dictionary that will be added to Redis
        # container_dict = dict()
        
        # add values to container dictionary; packages predicted and a timestamp from the container
        packages = []
        for key in results:
            packages.extend(results[key]) 
        
        # get final timestamp
        times = metrics_d[cname]['df']
        timestamp = times.index[0]
        
        # get container_id
        cid = metrics_d[cname]['container_id']

        # insert into redis
        mongo.insert_one({
            "name": cname,
            "container_id": cid,
            "packages": packages,
            "datetime": timestamp
        })
        
        print("=================")
        print(cname, timestamp)
        print(packages)
        print("=================")

    
gen_prediction_op = kfp.components.create_component_from_func(gen_prediction, output_component_file=None, base_image="rohankumar1/inference-image:0.01") 
    
def add_node_selector(label_name: str, label_value: str, container_op: dsl.ContainerOp) -> None:
    container_op.add_node_selector_constraint(label_name=label_name, label_values=label_value)

def use_image_pull_policy(image_pull_policy='Always'):
    def _use_image_pull_policy(task):
        task.container.set_image_pull_policy(image_pull_policy)
        return task
    return _use_image_pull_policy
    

@kfp.dsl.pipeline(
    name="Submitted Pipeline",
)
def praxium():
    # create affinity objects
    terms = kubernetes.client.models.V1NodeSelectorTerm(    # GPU nodes had permission issues, so we enforce to use other nodes. Use this code to set node selector.
        match_expressions=[
            {
                'key': 'kubernetes.io/hostname',
                'operator': 'In',
                'values': ['wrk-99']
            }
        ]
    )
    
    node_selector = kubernetes.client.models.V1NodeSelector(node_selector_terms=[terms])
    node_affinity = kubernetes.client.models.V1NodeAffinity(
        required_during_scheduling_ignored_during_execution=node_selector
    )
    affinity = kubernetes.client.models.V1Affinity(node_affinity=node_affinity)
    
    dsl.get_pipeline_conf().set_image_pull_secrets([kubernetes.client.V1ObjectReference(name="regcred22")])

    # Pipeline design
    model = generate_loadmod_op().apply(use_image_pull_policy()).add_affinity(affinity)
    change_test = generate_changeset_op("test").apply(use_image_pull_policy()).add_affinity(affinity)
    change_test.set_cpu_limit('5')
    change_test.set_cpu_request('5')
    change_test.set_memory_limit('5120Mi')
    change_test.set_memory_request('5120Mi')
    tag_test = generate_tagset_op(change_test.outputs["args"], change_test.outputs["cs"]).apply(use_image_pull_policy()).add_affinity(affinity)
    tag_test.set_cpu_limit('5')
    tag_test.set_cpu_request('5')
    tag_test.set_memory_limit('5120Mi')
    tag_test.set_memory_request('5120Mi')
    prediction = gen_prediction_op(model.outputs["clf"],model.outputs["index_tag_mapping"],model.outputs["tag_index_mapping"],model.outputs["index_label_mapping"],model.outputs["label_index_mapping"], tag_test.outputs["output_text"]).apply(use_image_pull_policy()).add_affinity(affinity)
    prediction.set_cpu_limit('5')
    prediction.set_cpu_request('5')
    prediction.set_memory_limit('5120Mi')
    prediction.set_memory_request('5120Mi')
    

if __name__ == "__main__":

    client = kfp_tekton.TektonClient(
            host=kubeflow_endpoint,
            existing_token=BEARER_TOK,
        )
    
    client.create_run_from_pipeline_func(praxium, arguments={}, experiment_name="praxium")