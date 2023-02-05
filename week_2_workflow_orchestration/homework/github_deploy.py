from prefect.filesystems import GitHub
from etl_we_2_gcs import etl_web_2_gcs
from prefect.deployments import Deployment

github_block = GitHub.load("github-store")

deploy_blcok = Deployment.build(
    flow= etl_web_2_gcs,
    name= "github ETL flow",
    infrastructure= github_block
)

if __name__=="__main__":
    deploy_block.apply()

    
