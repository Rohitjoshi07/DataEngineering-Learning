from prefect.filesystems import GitHub
from etl_web_to_gcs import etl_web_2_gcs
from prefect.deployments import Deployment
github_block = GitHub.load("github-store")

deploy_block = Deployment.build_from_flow(
    flow= etl_web_2_gcs,
    name= "github-ETL-flow",
    storage=github_block
)

if __name__=="__main__":
    deploy_block.apply()
