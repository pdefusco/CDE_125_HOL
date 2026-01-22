# Déploiement et Orchestration avec Airflow dans CDE

![texte alternatif](../../img/cicd-deployment.png)

## Contenu

3. [Promouvoir vers un environnement supérieur en utilisant l'API en répliquant le dépôt et en redéployant](https://github.com/pdefusco/CDE_124_HOL/blob/main/step_by_step_guides/english/03-deployment.md#lab-3-promote-to-higher-env-using-api-by-replicating-repo-and-redeploy)
4. [Construire un pipeline d'orchestration avec Airflow](https://github.com/pdefusco/CDE_124_HOL/blob/main/step_by_step_guides/english/03-deployment.md#lab-4-build-orchestration-pipeline-with-airflow)

## Lab 3. Promouvoir vers un environnement supérieur en utilisant l'API en répliquant le dépôt et en redéployant

Maintenant que le job a réussi, déployez-le dans votre cluster PRD.

Créez et synchronisez le même dépôt Git depuis le cluster PRD. Désormais, exécutez les commandes CLI suivantes en utilisant l'URL de l'API Jobs de votre cluster PRD comme paramètre `vcluster-endpoint`.

```
cde repository create \
  --name sparkAppRepoPrdUser001 \
  --branch main \
  --url https://github.com/pdefusco/CDE_124_HOL.git \
  --vcluster-endpoint <your-PRD-vc-jobs-api-url-here>
```

```
cde repository sync \
  --name sparkAppRepoPrdUser001 \
  --vcluster-endpoint <your-PRD-vc-jobs-api-url-here>
```

Ensuite, créez un job CDE Spark en utilisant le dépôt CDE comme dépendance.

```
cde job create --name cde_spark_job_prd_user001 \
  --type spark \
  --mount-1-resource sparkAppRepoPrdUser001 \
  --executor-cores 2 \
  --executor-memory "4g" \
  --application-file pyspark-app.py\
  --vcluster-endpoint <your-PRD-vc-jobs-api-url-here> \
  --arg <your-storage-location-here>
```

```
cde job run --name cde_spark_job_prd_user001 \
  --executor-cores 4 \
  --executor-memory "2g" \
  --vcluster-endpoint <your-PRD-vc-jobs-api-url-here>
```

![texte alternatif](../../img/move-job.png)

## Lab 4. Construire un pipeline d'orchestration avec Airflow

Créez les jobs CDE Spark. Notez que ceux-ci sont classés en Bronze, Silver et Gold suivant une architecture de données Lakehouse.

```
cde job create --name cde_spark_job_bronze_user001 \
  --type spark \
  --arg <your-cdp-workload-username-here> \
  --arg <your-storage-location-here> \
  --mount-1-resource sparkAppRepoPrdUser001 \
  --python-env-resource-name Python-Env-Shared \
  --executor-cores 2 \
  --executor-memory "4g" \
  --application-file de-pipeline/spark/001_Lakehouse_Bronze.py\
  --vcluster-endpoint <your-PRD-vc-jobs-api-url-here>
```

```
cde job create --name cde_spark_job_silver_user001 \
  --type spark \
  --arg <your-cdp-workload-username-here> \
  --mount-1-resource sparkAppRepoPrdUser001 \
  --python-env-resource-name Python-Env-Shared \
  --executor-cores 2 \
  --executor-memory "4g" \
  --application-file de-pipeline/spark/002_Lakehouse_Silver.py\
  --vcluster-endpoint <your-PRD-vc-jobs-api-url-here>
```

```
cde job create --name cde_spark_job_gold_user001 \
  --type spark \
  --arg <your-cdp-workload-username-here> \
  --arg <your-storage-location-here> \
  --mount-1-resource sparkAppRepoPrdUser001 \
  --python-env-resource-name Python-Env-Shared \
  --executor-cores 2 \
  --executor-memory "4g" \
  --application-file de-pipeline/spark/003_Lakehouse_Gold.py\
  --vcluster-endpoint <your-PRD-vc-jobs-api-url-here>
```

Dans votre éditeur, ouvrez le DAG Airflow "004_airflow_dag_git" et éditez votre variable `username` à la ligne 54.

![texte alternatif](../../img/username-dag.png)

Ensuite, créez le job CDE Airflow. Ce job orchestrera vos jobs Spark Lakehouse ci-dessus.

```
cde job create --name airflow-orchestration-user001 \
  --type airflow \
  --mount-1-resource sparkAppRepoPrdUser001 \
  --dag-file de-pipeline/airflow/004_airflow_dag_git.py\
  --vcluster-endpoint <your-PRD-vc-jobs-api-url-here>
```

![texte alternatif](../../img/jobs-cde.png)

![texte alternatif](../../img/jobs-in-ui.png)

Il n'est pas nécessaire de déclencher manuellement l'exécution du job Airflow. Les paramètres du DAG incluent déjà un horaire. Après sa création, le job CDE Airflow s'exécutera rapidement. Vous pouvez suivre l'avancement dans l'interface utilisateur des Exécutions de Jobs.

![texte alternatif](../../img/jobs-completed.png)

Vous pouvez utiliser l'interface Airflow pour inspecter vos pipelines. Depuis la page des détails du Cluster Virtuel, ouvrez l'interface Airflow et localisez votre DAG Airflow.

![texte alternatif](../../img/vcdetails.png)

![texte alternatif](../../img/open-your-dag.png)

![texte alternatif](../../img/dag-runs-page.png)

Airflow fournit une variété de diagrammes, graphiques et visuels pour surveiller vos exécutions à travers les tâches, les dags et les opérateurs. Exécutez votre DAG Airflow plusieurs fois depuis l'interface CDE Jobs et revenez à l'interface Airflow pour inspecter vos tâches sur différentes exécutions, etc.

![texte alternatif](../../img/trigger-dag.png)

![texte alternatif](../../img/airflow-details.png)

![texte alternatif](../../img/airflow-graphs.png)

![texte alternatif](../../img/airflow-task-compare.png)

CDE Airflow prend en charge les fournisseurs tiers, c'est-à-dire des packages externes qui étendent les fonctionnalités d'Apache Airflow en ajoutant des intégrations avec d'autres systèmes, services et outils tels qu'AWS, Google Cloud, Microsoft Azure, bases de données, courtiers de messages et bien d'autres services. Les fournisseurs sont open source et peuvent être installés séparément en fonction des besoins spécifiques d'un projet.

Sélectionnez la tâche GitHub List Repos, ouvrez les journaux et constatez que la sortie est fournie. Dans cette tâche particulière, vous avez utilisé l'opérateur GitHub pour lister les dépôts d'un compte GitHub.

![texte alternatif](../../img/airflow-github-list-repos.png)

Une connexion Airflow a été créée à l'avance pour se connecter à ce compte via un jeton GitHub. Ouvrez la page des connexions pour explorer d'autres connexions.

![texte alternatif](../../img/airflow-connections.png)

![texte alternatif](../../img/airflow-connections-2.png)

![texte alternatif](../../img/airflow-connections-3.png)

L'opérateur GitHub a été installé dans l'environnement Python Airflow du Cluster Virtuel. Revenez à la page des détails du Cluster Virtuel, ouvrez l'onglet Airflow et validez les packages installés.

![texte alternatif](../../img/airflow-installed-packages.png)

## Résumé et Prochaines Étapes

Apache Airflow est un outil open-source d'automatisation des workflows et d'orchestration conçu pour la planification, la surveillance et la gestion de pipelines de données complexes. Il permet aux utilisateurs de définir des workflows sous forme de Graphes Acycliques Dirigés (DAGs) en utilisant Python, offrant ainsi flexibilité, évolutivité et automatisation dans le traitement des données. Avec des intégrations natives, une interface web conviviale et de solides capacités d'exécution de tâches, Airflow est largement utilisé dans l'ingénierie des données, les processus ETL et les pipelines d'apprentissage automatique.

CDE intègre Apache Airflow au niveau du Cluster Virtuel CDE. Il est automatiquement déployé pour l'utilisateur CDE lors de la création du Cluster Virtuel CDE et ne nécessite aucune maintenance de la part de l'administrateur CDE.

Dans cette section des laboratoires, nous avons déployé un pipeline Spark et Iceberg avec des dépôts git et CDE, et créé un pipeline d'orchestration de jobs avec Airflow. Vous pouvez également trouver les articles et démonstrations suivants pertinents :

* [Documentation d'Airflow dans CDE](https://docs.cloudera.com/cdp-private-cloud-upgrade/latest/cdppvc-data-migration-spark/topics/cdp-migration-spark-cde-airflow-overview.html)
* [Utilisation d'Airflow dans CDE](https://docs.cloudera.com/cdp-private-cloud-upgrade/latest/cdppvc-data-migration-spark/topics/cdp-migration-spark-cde-using-airflow.html)
* [Créer un dépôt CDE dans CDE](https://docs.cloudera.com/data-engineering/1.5.4/manage-jobs/topics/cde-git-repo.html)
