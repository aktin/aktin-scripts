import os.path
import subprocess
import xml.etree.ElementTree as ET
from subprocess import CompletedProcess

# set docker container id if EAR is running inside a docker container (or is linked as volume)
# after successful build the script attempts a restart of the container
docker_container_id = ""

# set absolute path to the project root / parent folder
# if folder does not exist, it will be created
dwh_projects_root = "/path/to/root"

# set start project (e.g. dwh-admin) to only build the target and all subsequential projects
# minimizes build time
# alternatively if the start project is empty and the script is called from a project folder, it uses that project as entry point
# otherwise it builds every project
start_project = ""

github_url_template = "git@github.com:{0}/{1}.git"
aktin_project_name = "aktin"
aktin_project_path = os.path.join(dwh_projects_root, aktin_project_name)

pom_namespace = {"pom": "http://maven.apache.org/POM/4.0.0"}

# prepare folder structure
if not os.path.exists(dwh_projects_root):
    os.makedirs(dwh_projects_root)

# get DWH central project if not exists and get the names of all child projects
if not os.path.exists(os.path.join(aktin_project_path, "pom.xml")):
    subprocess.call(["git", "clone", github_url_template.format(aktin_project_name, aktin_project_name), aktin_project_path])

aktin_project_path = os.path.join(dwh_projects_root, aktin_project_name)

dwh_xml = ET.parse(os.path.join(aktin_project_path, "dwh.xml"))

projects = [p.text for p in dwh_xml.findall("./pom:modules/pom:module", pom_namespace)]

if not start_project:
    start_project = os.path.basename(os.getcwd())
    if not start_project in projects:
        start_project = projects[0]

# pull projects from github if it does not exist and build it
for i, project in enumerate(projects):
    # skip if start_project is after project in the project order
    try:
        if i < projects.index(start_project):
            continue
    except ValueError:
        pass

    project_path = os.path.join(dwh_projects_root, project)
    if not os.path.exists(os.path.join(dwh_projects_root, project)):
        subprocess.run(["git", "clone", github_url_template.format(aktin_project_name, project)],
                        cwd=dwh_projects_root)

    completed_process: CompletedProcess = subprocess.run(["mvn", "clean", "install"], cwd=project_path)

    if completed_process.returncode != 0:
        exit(1)

if docker_container_id:
    subprocess.run(["docker", "restart", docker_container_id])