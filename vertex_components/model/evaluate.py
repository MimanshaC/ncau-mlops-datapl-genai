from kfp.v2.dsl import Artifact
from kfp.v2.dsl import Input
from kfp.v2.dsl import Metrics
from kfp.v2.dsl import Model
from kfp.v2.dsl import Output
from kfp.v2.dsl import component

from config import BASE_IMAGE


@component(base_image=BASE_IMAGE)
def evaluate(
    project: str,
    dataset: Input[Artifact],
    target_column: str,
    model: Input[Model],
    metrics: Output[Metrics],
) -> None:  # type: ignore
    """Component to run evaluation as part of Vertex AI pipeline
    All relevant libraries need to be imported within the component function;
    otherwise they won't be found!

    Args:
        project (str): project ID
        dataset (Input[Artifact]): dataset artifact
        target_column (str): column name of target varibale
        model (Input[Model]): Vertex model as Model artifact to evaluate
        metrics (Output[Metrics]): metrics as output Artifact of component
    """
    from xgb_churn_prediction.data import data_ingestion
    from xgb_churn_prediction.model import evaluate
    from xgb_churn_prediction.model import save_load_model

    # Read in training data
    sql_query = f"""
        SELECT * EXCEPT(split)
        FROM `{dataset.metadata["datasetId"]}.{dataset.metadata["tableId"]}`
        WHERE split = 'TEST'
    """
    test_data_df = data_ingestion.execute_bq_query(project, sql_query)

    trained_model = save_load_model.load_model(str(model.path))

    evals = evaluate.evaluate_model(test_data_df, trained_model, target_column)

    # log metrics to metric output
    for metric, value in evals.items():
        metrics.log_metric(metric, value)


@component(base_image=BASE_IMAGE)
def champion_challenger_classification(
    project_id: str,
    location: str,
    model_name: str,
    pipeline_job_name: str,
    vertex_model: Input[Artifact],
    metrics: Output[Metrics],
) -> None:
    """Component to run champion challenger comparison and write results to cloud monitoring

    Args:
        project (str): project id
        location (str): location
        model_name (str): model name
        pipeline_job_name (str): pipeline job name
        vertex_model (str): versioned model uri of just trained model
        metrics (Output[Metrics]): Metrics output artifact generated by Vertex
    """
    # import all necessary libraries within component
    import logging
    import operator
    from datetime import datetime
    from datetime import timezone

    from google.cloud import aiplatform

    from xgb_churn_prediction.model import evaluate
    from xgb_churn_prediction.monitoring.metrics import (
        write_metrics_to_cloud_monitoring,
    )

    # TODO: Update to relevant metrics
    metrics_dict = {"f1Score": operator.ge, "precision": operator.ge}
    logging.info("Loading evaluations for champion and challenger model")
    try:
        # load current model + latest metrics
        current_model = aiplatform.Model(model_name=vertex_model.uri)
        current_metrics = current_model.get_model_evaluation().to_dict()["metrics"][
            "confidenceMetrics"
        ][
            0
        ]  # noqa

        # load challenger model (label=default, tbd) + latest metrics
        model_uri = vertex_model.uri.split("@")[0]
        champion_model = aiplatform.Model(model_name=model_uri, version="default")
        metrics_champion = champion_model.get_model_evaluation().to_dict()["metrics"][
            "confidenceMetrics"
        ][
            0
        ]  # noqa
    except Exception as e:
        raise Warning(f"Model evaluations for at least one model not found. Error {e}")

    # run comparison of metrics
    logging.info(f"Comparing champion and challenger model based on {list(metrics_dict.keys())}")
    improved = evaluate.champion_challenger(
        metrics_champion=metrics_champion,
        metrics_challenger=current_metrics,
        metrics_dict=metrics_dict,
    )

    logging.info(f"Logging metric 'improved' = {improved} as part of pipeline")
    # add champion challenger results to metrics dict and log in metric output
    current_metrics.update({"improved_against_champion": improved})
    metrics.log_metric("improved_against_champion", improved)

    # write metrics to cloud monitoring
    logging.info("Writing metrics to cloud monitoring")
    now = datetime.now(tz=timezone.utc)
    write_metrics_to_cloud_monitoring(
        project=project_id,
        location=location,
        model_name=model_name,
        pipeline_job_name=pipeline_job_name,
        timestamp=now,
        metrics=current_metrics,
        prefix="model_evaluation",
    )


@component(base_image=BASE_IMAGE)
def champion_challenger_regression(
    project_id: str,
    location: str,
    model_name: str,
    pipeline_job_name: str,
    vertex_model: Input[Artifact],
    metrics: Output[Metrics],
) -> None:
    """Component to run champion challenger comparison and write results to cloud monitoring

    Args:
        project (str): project id
        location (str): location
        model_name (str): model name
        pipeline_job_name (str): pipeline job name
        vertex_model (str): versioned model uri of just trained model
        metrics (Output[Metrics]): Metrics output artifact generated by Vertex
    """
    # import all necessary libraries within component
    import logging
    import operator
    from datetime import datetime
    from datetime import timezone

    from google.cloud import aiplatform

    from xgb_churn_prediction.model import evaluate
    from xgb_churn_prediction.monitoring.metrics import (
        write_metrics_to_cloud_monitoring,
    )

    # TODO: Update to relevant metrics
    metrics_dict = {"meanAbsoluteError": operator.le, "rSquared": operator.le}

    logging.info("Loading evaluations for champion and challenger model")
    try:
        # load current model + latest metrics
        current_model = aiplatform.Model(model_name=vertex_model.uri)
        current_metrics = current_model.get_model_evaluation().to_dict()["metrics"]  # noqa

        # load challenger model (label=default, tbd) + latest metrics
        model_uri = vertex_model.uri.split("@")[0]
        champion_model = aiplatform.Model(model_name=model_uri, version="default")
        metrics_champion = champion_model.get_model_evaluation().to_dict()["metrics"]  # noqa
    except Exception as e:
        raise Warning(f"Model evaluations for at least one model not found. Error {e}")

    # run comparison of metrics
    logging.info(f"Comparing champion and challenger model based on {list(metrics_dict.keys())}")
    improved = evaluate.champion_challenger(
        metrics_champion=metrics_champion,
        metrics_challenger=current_metrics,
        metrics_dict=metrics_dict,
    )

    logging.info(f"Logging metric 'improved' = {improved} as part of pipeline")
    # add champion challenger results to metrics dict and log in metric output
    current_metrics.update({"improved_against_champion": improved})
    metrics.log_metric("improved_against_champion", improved)

    # write metrics to cloud monitoring
    logging.info("Writing metrics to cloud monitoring")
    now = datetime.now(tz=timezone.utc)
    write_metrics_to_cloud_monitoring(
        project=project_id,
        location=location,
        model_name=model_name,
        pipeline_job_name=pipeline_job_name,
        timestamp=now,
        metrics=current_metrics,
        prefix="model_evaluation",
    )


@component(base_image=BASE_IMAGE)
def upload_evaluations_classification(
    metrics: Input[Metrics], vertex_model: Input[Artifact], service_endpoint: str
) -> None:
    """Component to upload evaluation as part of Vertex AI pipeline
    All relevant libraries need to be imported within the component function;
    otherwise they won't be found!

    Args:
        metrics (Input[Metrics]): metrics to upload
        vertex_model (Input[Artifact]): relevant vertex model to upload evaluations for
        service_endpoint (str): service endpoint to initiate a ModelServiceClient
    """
    from google.cloud.aiplatform import gapic

    # add metrics based on metrics schema here
    # all pre-built metrics schemas can be found here:
    # https://console.cloud.google.com/storage/browser/google-cloud-aiplatform/schema/modelevaluation;tab=objects?prefix=&forceOnObjectsSortingFiltering=false # noqa
    # e.g. classification metrics:
    metrics_dict = {}
    metrics_dict["confidenceMetrics"] = [
        {
            "f1Score": metrics.metadata["f1score"],
            "precision": metrics.metadata["precision"],
            "recall": metrics.metadata["recall"],
        }
    ]
    # generate model evaluation to upload by stating metrics_schema_uri and metrics
    # they have to match up to make the upload work, i.e. only use metrics defined in schema
    model_eval = gapic.ModelEvaluation(
        display_name="eval",
        metrics_schema_uri="gs://google-cloud-aiplatform/schema/modelevaluation/classification_metrics_1.0.0.yaml",  # noqa
        metrics=metrics_dict,
    )
    # upload evaluation to uploaded Vertex model (versioned uri)
    client = gapic.ModelServiceClient(client_options={"api_endpoint": service_endpoint})
    client.import_model_evaluation(parent=vertex_model.uri, model_evaluation=model_eval)


@component(base_image=BASE_IMAGE)
def upload_evaluations_regression(
    metrics: Input[Metrics], vertex_model: Input[Artifact], service_endpoint: str
) -> None:
    """Component to upload evaluation as part of Vertex AI pipeline
    All relevant libraries need to be imported within the component function;
    otherwise they won't be found!

    Args:
        metrics (Input[Metrics]): metrics to upload
        vertex_model (Input[Artifact]): relevant vertex model to upload evaluations for
        service_endpoint (str): service endpoint to initiate a ModelServiceClient
    """
    from google.cloud.aiplatform import gapic

    # add metrics based on metrics schema (regression metrics)
    metrics_dict = {
        "rootMeanSquaredError": metrics.metadata["rmse"],
        "meanAbsoluteError": metrics.metadata["mae"],
        "rSquared": metrics.metadata["r2"],
    }

    # generate model evaluation to upload by stating metrics_schema_uri and metrics
    # they have to match up to make the upload work, i.e. only use metrics defined in schema
    model_eval = gapic.ModelEvaluation(
        display_name="eval",
        metrics_schema_uri="gs://google-cloud-aiplatform/schema/modelevaluation/regression_metrics_1.0.0.yaml",  # noqa
        metrics=metrics_dict,
    )
    # upload evaluation to uploaded Vertex model (versioned uri)
    client = gapic.ModelServiceClient(client_options={"api_endpoint": service_endpoint})
    client.import_model_evaluation(parent=vertex_model.uri, model_evaluation=model_eval)
