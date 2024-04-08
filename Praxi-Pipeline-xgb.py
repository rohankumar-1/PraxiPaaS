

kubeflow_endpoint="https://ds-pipeline-pipelines-definition-ai4cloudops-11855c.apps.shift.nerc.mghpcc.org"
bearer_token = "sha256~0BJfe202nyTu6hCk35UEGv4Z_-uSrC56KY_6mAo7xDI" # oc whoami --show-token

from typing import NamedTuple

import os
import kfp, kfp_tekton, kubernetes
import kfp.dsl as dsl
from kfp.components import InputPath, InputTextFile, OutputPath, OutputTextFile
from kfp.components import func_to_container_op

os.environ["DEFAULT_STORAGE_CLASS"] = "ocs-external-storagecluster-ceph-rbd"
os.environ["DEFAULT_ACCESSMODES"] = "ReadWriteOnce"

def load_model(clf_path: OutputPath(str), index_tag_mapping_path: OutputPath(str), tag_index_mapping_path: OutputPath(str), index_label_mapping_path: OutputPath(str), label_index_mapping_path: OutputPath(str)):
    '''Loads the vw model file and Hybrid class object '''
    import boto3
    import os
    import time
    # time.sleep(50000)
    
    s3 = boto3.resource(service_name='s3', 
                        region_name='us-east-1', 
                        aws_access_key_id="AKIAXECNQISLAUNL67HV", 
                        aws_secret_access_key="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",)

    s3.Bucket('praxi-model-xgb-02').download_file(Key='True25_1000submodel_verpak.zip', Filename=clf_path)
    # # time.sleep(50000)

generate_loadmod_op = kfp.components.create_component_from_func(load_model, output_component_file='generate_loadmod_op.yaml', base_image="zongshun96/load_model_s3:0.01")


def generate_changesets(user_in: str, cs_path: OutputPath(str), args_path: OutputPath(str)):
    import read_layered_image
    import pickle
    import time
    import yaml
    import boto3
    # import os
    # import json
    s3 = boto3.resource(service_name='s3', 
                        region_name='us-east-1', 
                        aws_access_key_id="AKIAXECNQISLAUNL67HV", 
                        aws_secret_access_key="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",)
    
    changesets_l = read_layered_image.run()
    # time.sleep(5000)
    # debug
    for ind, changeset in enumerate(changesets_l):
        with open("/pipelines/component/cwd/changesets/changesets_l"+str(ind)+".yaml", 'w') as writer:
            # yaml.dump(changesets_l, writer)
            yaml.dump(changeset, writer, default_flow_style=False)
        s3.Bucket('praxi-interm-1').upload_file("/pipelines/component/cwd/changesets/changesets_l"+str(ind)+".yaml", "changesets_l"+str(ind)+".yaml")
    # pass data to next component
    with open(cs_path, 'wb') as writer:
        pickle.dump(changesets_l, writer)
    with open(args_path, 'wb') as argfile:
        pickle.dump(user_in, argfile)
    # time.sleep(5000)
generate_changeset_op = kfp.components.create_component_from_func(generate_changesets, output_component_file='generate_changeset_component.yaml', base_image="zongshun96/prom-get-layers:0.03")

def generate_tagset(input_args_path: InputPath(str), changeset_path: InputPath(str), output_text_path: OutputPath(str), output_args_path: OutputPath(str)):
    '''generate tagset from the changeset'''
    # import tagset_gen
    from columbus.columbus import columbus
    import json
    import pickle
    import os
    import time
    import boto3
    # from function import changeset_gen
    s3 = boto3.resource(service_name='s3', 
                        region_name='us-east-1', 
                        aws_access_key_id="AKIAXECNQISLAUNL67HV", 
                        aws_secret_access_key="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",)

    # Load data from previous component
    with open(input_args_path, 'rb') as in_argfile:
        user_in = pickle.load(in_argfile)
    with open(changeset_path, 'rb') as in_changesets_l:
        changesets_l = pickle.load(in_changesets_l)
                              
    # Tagset Generator
    tagsets_l = []
    for changeset in changesets_l:
        # tags = tagset_gen.get_columbus_tags(changeset['changes'])
        tag_dict = columbus(changeset['changes'], freq_threshold=2)
        tags = ['{}:{}'.format(tag, freq) for tag, freq in tag_dict.items()]
        cur_dict = {'labels': changeset['labels'], 'tags': tags}
        tagsets_l.append(cur_dict)

    # Debug
    with open("/pipelines/component/cwd/changesets_l_dump", 'w') as writer:
        for change_dict in changesets_l:
            writer.write(json.dumps(change_dict) + '\n')
    for ind, tag_dict in enumerate(tagsets_l):
        with open("/pipelines/component/cwd/tagsets_"+str(ind)+".tag", 'w') as writer:
            writer.write(json.dumps(tag_dict) + '\n')
        s3.Bucket('praxi-interm-1').upload_file("/pipelines/component/cwd/tagsets_"+str(ind)+".tag", "tagsets_"+str(ind)+".tag")
    # time.sleep(5000)

    # Pass data to next component
    # for ind, tag_dict in enumerate(tagsets_l):
    #     with open(output_text_path+"/tagsets_"+str(ind)+".tag", 'w') as writer:
    #         writer.write(json.dumps(tag_dict) + '\n')
    with open(output_text_path, 'wb') as writer:
        # for tag_dict in tag_dict_gen:
        #     writer.write(json.dumps(tag_dict) + '\n')
        pickle.dump(tagsets_l, writer)
    with open(output_args_path, 'wb') as argfile:
        pickle.dump(user_in, argfile)
generate_tagset_op = kfp.components.create_component_from_func(generate_tagset, output_component_file='generate_tagset_component.yaml', base_image="zongshun96/taggen_openshift:0.01")


def gen_prediction(clf_zip_path: InputPath(str), test_tags_path: InputPath(str), prediction_path: OutputPath(str)):
# def gen_prediction(model_path: InputPath(str), modfile_path: InputPath(str), test_tags_path: InputPath(str), created_tags_path: InputPath(str), prediction_path: OutputPath(str)):
    '''generate prediction given model'''
    # import main
    import zipfile
    import os, sys
    import yaml
    import pickle
    import time
    import tagsets_XGBoost
    import xgboost as xgb
    import boto3
    import numpy as np
    import tqdm
    import multiprocessing as mp
    from collections import defaultdict
    # time.sleep(5000)

    # args = main.get_inputs()
    s3 = boto3.resource(service_name='s3', 
                        region_name='us-east-1', 
                        aws_access_key_id="AKIAXECNQISLAUNL67HV", 
                        aws_secret_access_key="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",)
    cwd = "/pipelines/component/cwd/"
    # cwd = "/home/ubuntu/Praxi-Pipeline/prediction_XGBoost_openshift_image/model_testing_scripts/cwd/"



    # Path to the zip file (include the full path if the file is not in the current directory)
    zip_file_path = clf_zip_path
    # Directory where you want to extract the files
    extract_to_dir = cwd
    # Check if the extraction directory exists, if not, create it
    if not os.path.exists(extract_to_dir):
        os.makedirs(extract_to_dir)
    # Unzipping the file
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to_dir)
    print(f'Files extracted to {extract_to_dir}')

    dataset = "data_4"
    n_models = 50
    shuffle_idx = 0
    test_sample_batch_idx = 0
    n_samples = 4
    clf_njobs = 32
    n_estimators = 100
    depth = 1
    input_size = None
    dim_compact_factor = 1
    tree_method = "exact"
    max_bin = 1
    with_filter = True
    freq = 25


    # Data 
    # tag_files_l = [tag_file for tag_file in os.listdir(test_tags_path) if tag_file[-3:] == 'tag']
    # tag_files_l_of_l, step = [], len(tag_files_l)//mp.cpu_count()+1
    # for i in range(0, len(tag_files_l), step):
    #     tag_files_l_of_l.append(tag_files_l[i:i+step])
    # pool = mp.Pool(processes=1)
    # data_instance_d_l = [pool.apply_async(tagsets_XGBoost.map_tagfilesl, args=(test_tags_path, tag_files_l, cwd, True, freq)) for tag_files_l in tqdm(tag_files_l_of_l)]
    # data_instance_d_l = [data_instance_d.get() for data_instance_d in tqdm(data_instance_d_l) if data_instance_d.get()!=None]
    # pool.close()
    # pool.join()
    # all_tags_set, all_label_set = set(), set()
    # tags_by_instance_l, labels_by_instance_l = [], []
    # tagset_files = []
    # for data_instance_d in data_instance_d_l:
    #     if len(data_instance_d) == 5:
    #             tagset_files.extend(data_instance_d['tagset_files'])
    #             all_tags_set.update(data_instance_d['all_tags_set'])
    #             tags_by_instance_l.extend(data_instance_d['tags_by_instance_l'])
    #             all_label_set.update(data_instance_d['all_label_set'])
    #             labels_by_instance_l.extend(data_instance_d['labels_by_instance_l'])
    
    all_tags_set, all_label_set = set(), set()
    tags_by_instance_l, labels_by_instance_l = [], []
    tagset_files = []
    with open(test_tags_path, 'rb') as reader:
        tagsets_l = pickle.load(reader)
        for tagset in tagsets_l:
            tagset_files.append("filename")
            instance_feature_tags_d = defaultdict(int)
            # feature 
            for tag_vs_count in tagset['tags']:
                k,v = tag_vs_count.split(":")
                all_tags_set.add(k)
                instance_feature_tags_d[k] += int(v)
            tags_by_instance_l.append(instance_feature_tags_d)
            # label
            if 'labels' in tagset:
                all_label_set.update(tagset['labels'])
                labels_by_instance_l.append(tagset['labels'])
            else:
                all_label_set.add(tagset['label'])
                labels_by_instance_l.append([tagset['label']])


    # Models
    clf_path_l = []
    for i in range(n_models):
        clf_pathname = f"{cwd}/cwd_ML_with_"+dataset+"_"+str(n_models)+"_"+str(i)+"_train_"+str(shuffle_idx)+"shuffleidx_"+str(test_sample_batch_idx)+"testsamplebatchidx_"+str(n_samples)+"nsamples_"+str(clf_njobs)+"njobs_"+str(n_estimators)+"trees_"+str(depth)+"depth_"+str(input_size)+"-"+str(dim_compact_factor)+"rawinput_sampling1_"+str(tree_method)+"treemethod_"+str(max_bin)+"maxbin_modize_par_"+str(with_filter)+f"{freq}removesharedornoisestags_verpak/model_init.json"
        if os.path.isfile(clf_pathname):
            clf_path_l.append(clf_pathname)
        else:
            print(f"clf is missing: {clf_pathname}")
            sys.exit(-1)

    # Make inference
    results = defaultdict(list)
    for clf_idx, clf_path in enumerate(clf_path_l):
        BOW_XGB = xgb.XGBClassifier(max_depth=10, learning_rate=0.1,silent=False, objective='binary:logistic', \
                        booster='gbtree', n_jobs=8, nthread=None, gamma=0, min_child_weight=1, max_delta_step=0, \
                        subsample=0.8, colsample_bytree=0.8, colsample_bylevel=0.8, reg_alpha=0, reg_lambda=1)
        BOW_XGB.load_model(clf_path)
        BOW_XGB.set_params(n_jobs=1)
        feature_importance = BOW_XGB.feature_importances_

        tag_files_l = [tag_file for tag_file in os.listdir(test_tags_path) if tag_file[-3:] == 'tag']
        step = len(tag_files_l)
        for batch_first_idx in range(0, len(tag_files_l), step):
            tagset_files_used, feature_matrix, label_matrix, instance_row_idx_set, instance_row_count = tagsets_XGBoost.tagsets_to_matrix(test_tags_path, tag_files_l = tag_files_l[batch_first_idx:batch_first_idx+step], cwd=clf_path[:-15], all_tags_set=all_tags_set,all_label_set=all_label_set,tags_by_instance_l=tags_by_instance_l,labels_by_instance_l=labels_by_instance_l,tagset_files=tagset_files, feature_importance=feature_importance)
            # tagset_files, feature_matrix, label_matrix = tagsets_XGBoost.tagsets_to_matrix(test_tags_path, index_tag_mapping_path, tag_index_mapping_path, index_label_mapping_path, label_index_mapping_path, train_flag=False, cwd=cwd)
            if feature_matrix.size != 0:
                # prediction
                pred_label_matrix = BOW_XGB.predict(feature_matrix)
                results = tagsets_XGBoost.merge_preds(results, tagsets_XGBoost.one_hot_to_names(f"{clf_path[:-15]}index_label_mapping", pred_label_matrix))

        
        print("clf"+str(clf_idx)+" pred done")

    # Pass data to next component
    with open(prediction_path, 'wb') as writer:
        pickle.dump(results, writer) 
    with open(cwd+"pred_l_dump", 'w') as writer:
        for pred in results.values():
            writer.write(f"{pred}\n")
    with open(cwd+"pred_d_dump", 'w') as writer:
        results_d = {}
        for k,v in results.items():
            results_d[int(k)] = v
        yaml.dump(results_d, writer)
    s3.Bucket('praxi-interm-1').upload_file(cwd+"pred_l_dump", "pred_l_dump")
    s3.Bucket('praxi-interm-1').upload_file(cwd+"pred_d_dump", "pred_d_dump")

    # debug
    # time.sleep(5000)
gen_prediction_op = kfp.components.create_component_from_func(gen_prediction, output_component_file='generate_pred_component.yaml', base_image="zongshun96/prediction_xgb_openshift:0.01") 


# # Reading bigger data
# @func_to_container_op
# def print_text(text_path: InputPath()): # The "text" input is untyped so that any data can be printed
#     '''Print text'''
#     with open(text_path, 'rb') as reader:
#         for line in reader:
#             print(line, end = '')
    
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
def praxi_pipeline():
    # vop = dsl.VolumeOp(
    #     name="interm-pvc",
    #     resource_name="interm-pvc",
    #     size="1Gi",
    #     modes=dsl.VOLUME_MODE_RWM,
    #     volume_name="pvc-75829191-2c57-4630-ae3b-191c4d4d372f",
    #     storage_class="manual",
    #     generate_unique_name=False,
    #     action='apply',
    #     set_owner_reference=True
    # )




    # kubernetes.config.load_kube_config()
    # api = kubernetes.client.AppsV1Api()

    # # read current state
    # deployment = api.read_namespaced_deployment(name='foo', namespace='bar')

    # check current state
    #print(deployment.spec.template.spec.affinity)

    # create affinity objects
    terms = kubernetes.client.models.V1NodeSelectorTerm(    # GPU nodes had permission issues, so we enforce to use other nodes. Use this code to set node selector.
        match_expressions=[
            {'key': 'kubernetes.io/hostname',
            'operator': 'NotIn',
            'values': ["wrk-10", "wrk-11"]}
        ]
    )
    node_selector = kubernetes.client.models.V1NodeSelector(node_selector_terms=[terms])
    node_affinity = kubernetes.client.models.V1NodeAffinity(
        required_during_scheduling_ignored_during_execution=node_selector
    )
    affinity = kubernetes.client.models.V1Affinity(node_affinity=node_affinity)


    # Pipeline design
    model = generate_loadmod_op().apply(use_image_pull_policy()).add_affinity(affinity)
    change_test = generate_changeset_op("test").apply(use_image_pull_policy()).add_affinity(affinity)
    change_test.set_cpu_limit('4')
    change_test.set_memory_limit('4096Mi')
    tag_test = generate_tagset_op(change_test.outputs["args"], change_test.outputs["cs"]).apply(use_image_pull_policy()).add_affinity(affinity)
    prediction = gen_prediction_op(model.outputs["clf"],model.outputs["index_tag_mapping"],model.outputs["tag_index_mapping"],model.outputs["index_label_mapping"],model.outputs["label_index_mapping"], tag_test.outputs["output_text"]).apply(use_image_pull_policy()).add_affinity(affinity)

if __name__ == "__main__":

    client = kfp_tekton.TektonClient(
            host=kubeflow_endpoint,
            existing_token=bearer_token,
            # ssl_ca_cert = '/home/ubuntu/cert/ca.crt'
        )
    # client = kfp.Client(host=kfp_endpoint)
    client.create_run_from_pipeline_func(praxi_pipeline, arguments={})
    # print(client.list_experiments())