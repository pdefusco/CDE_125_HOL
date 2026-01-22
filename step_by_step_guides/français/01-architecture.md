# Architecture CDE

## Objectif

Dans cette section, vous apprendrez l'architecture flexible de CDE et ses principaux composants.

## Table des matières

* [Introduction au service CDE](https://github.com/pdefusco/CDE119_ACE_WORKSHOP/blob/main/step_by_step_guides/english/part01_cde_architecture.md#introduction-to-the-cde-service)
  * [Environnement CDP](https://github.com/pdefusco/CDE119_ACE_WORKSHOP/blob/main/step_by_step_guides/english/part01_cde_architecture.md#cdp-environment)
  * [Service CDP](https://github.com/pdefusco/CDE119_ACE_WORKSHOP/blob/main/step_by_step_guides/english/part01_cde_architecture.md#cde-service)
  * [Cluster Virtuel](https://github.com/pdefusco/CDE119_ACE_WORKSHOP/blob/main/step_by_step_guides/english/part01_cde_architecture.md#virtual-cluster)
  * [Jobs CDE](https://github.com/pdefusco/CDE119_ACE_WORKSHOP/blob/main/step_by_step_guides/english/part01_cde_architecture.md#jobs)
  * [Ressource CDE](https://github.com/pdefusco/CDE119_ACE_WORKSHOP/blob/main/step_by_step_guides/english/part01_cde_architecture.md#resource)
  * [Exécution de Job](https://github.com/pdefusco/CDE119_ACE_WORKSHOP/blob/main/step_by_step_guides/english/part01_cde_architecture.md#job-run)
  * [Sessions CDE](https://github.com/pdefusco/CDE119_ACE_WORKSHOP/blob/main/step_by_step_guides/english/part01_cde_architecture.md#cde-session)
  * [Apache Iceberg](https://github.com/pdefusco/CDE119_ACE_WORKSHOP/blob/main/step_by_step_guides/english/part01_cde_architecture.md#apache-iceberg)
  * [Interface Utilisateur CDE](https://github.com/pdefusco/CDE119_ACE_WORKSHOP/blob/main/step_by_step_guides/english/part01_cde_architecture.md#cde-user-interface)
* [Résumé](https://github.com/pdefusco/CDE119_ACE_WORKSHOP/blob/main/step_by_step_guides/english/part01_cde_architecture.md#summary)

## Introduction au service CDE

Cloudera Data Engineering (CDE) est un service pour Cloudera Data Platform qui vous permet de soumettre des jobs batch sur des clusters virtuels à mise à l'échelle automatique. CDE vous permet de passer plus de temps sur vos applications et moins de temps sur l'infrastructure.

Cloudera Data Engineering vous permet de créer, gérer et planifier des jobs Apache Spark sans la charge de créer et maintenir des clusters Spark. Avec Cloudera Data Engineering, vous définissez des clusters virtuels avec une gamme de ressources CPU et mémoire, et le cluster se met à l'échelle selon les besoins pour exécuter vos charges de travail Spark, ce qui permet de contrôler vos coûts cloud.

Le service CDE peut être atteint depuis la page d'accueil de CDP en cliquant sur l'icône bleue "Data Engineering".

![alt text](../../img/cdp_lp_0.png)

La page d'accueil CDE vous permet d'accéder, de créer et de gérer des services CDE et des clusters virtuels. Dans chaque service CDE, vous pouvez déployer un ou plusieurs clusters virtuels CDE. Dans le cluster virtuel, vous pouvez créer, surveiller et résoudre les problèmes des tâches Spark et Airflow.

Le service CDE est lié à l'environnement CDP. Chaque service CDE est mappé à au plus un environnement CDP, tandis qu'un environnement CDP peut être mappé à un ou plusieurs services CDE.

Voici les composants les plus importants dans le service CDE :

##### Environnement CDP
Un sous-ensemble logique de votre compte fournisseur de cloud, incluant un réseau virtuel spécifique. Les environnements CDP peuvent être dans AWS, Azure, RedHat OCP et Cloudera ECS. Pour plus d'informations, consultez [Environnements CDP](https://docs.cloudera.com/management-console/cloud/overview/topics/mc-core-concepts.html). En pratique, un environnement est équivalent à un Data Lake, car chaque environnement est automatiquement associé à ses propres services SDX pour la sécurité, la gouvernance et la lignée.

##### Service CDE
Le cluster Kubernetes en fonctionnement et les services qui gèrent les clusters virtuels. Le service CDE doit être activé sur un environnement avant que vous puissiez créer des clusters virtuels.

##### Cluster Virtuel
Un cluster à mise à l'échelle automatique individuel avec des plages prédéfinies de CPU et de mémoire. Les Clusters Virtuels dans CDE peuvent être créés et supprimés à la demande. Les tâches sont associées aux clusters. Lors du déploiement d'un Cluster Virtuel, vous pouvez choisir entre deux niveaux de cluster:

*Core (Niveau 1)* : Les options de transformation et d'ingénierie basées sur le batch incluent :
* Cluster à mise à l'échelle automatique
* Instances Spot
* SDX/Lakehouse
* Cycle de vie des jobs
* Surveillance
* Orchestration des flux de travail

*All Purpose (Niveau 2)* : Développez en utilisant des sessions interactives et déployez des charges de travail batch et de streaming. Cette option inclut toutes les options du niveau 1 avec l'ajout des éléments suivants :
* Sessions Shell - CLI et Web
* JDBC/SparkSQL (Disponible en octobre 2023 avec CDE 1.20)
* IDE (Disponible en octobre 2023 avec CDE 1.20)

Les clusters Core sont recommandés pour les environnements de production. Les clusters All Purpose sont plutôt conçus pour être utilisés comme environnements de développement et de test.
Pour plus d'informations sur les versions 1.19.1 et 1.19.2 de CDE, veuillez consulter cette page dans la [documentation](https://docs.cloudera.com/data-engineering/cloud/release-notes/topics/cde-whats-new-1.19.html).

##### Jobs
Le code de l'application ainsi que les configurations et ressources définies. Les jobs peuvent être exécutés à la demande ou planifiés. Une exécution individuelle de job est appelée "job run".

##### Ressource
Une collection définie de fichiers, tels qu'un fichier Python ou une application JAR, des dépendances, et tout autre fichier de référence requis pour un job.

##### Exécution de Job
Une exécution individuelle de job.

##### Session CDE

Les sessions interactives CDE offrent aux ingénieurs de données des points de terminaison flexibles pour commencer à développer des applications Spark de n'importe où — dans un terminal web, une CLI locale, un IDE préféré, et même via JDBC à partir d'outils tiers.

##### Apache Iceberg

Apache Iceberg est un format de table open-source cloud-native et haute performance pour organiser des ensembles de données analytiques à l'échelle du pétaoctet sur un système de fichiers ou un stockage d'objets. Associé à Cloudera Data Platform (CDP), les utilisateurs peuvent construire une architecture de lac de données ouvert pour des analyses multi-fonctions et déployer des pipelines de bout en bout à grande échelle.

Le Data Lakehouse ouvert sur CDP simplifie l'analyse avancée de toutes les données avec une plateforme unifiée pour les données structurées et non structurées et des services de données intégrés pour permettre n'importe quel cas d'utilisation analytique, du ML, BI à l'analyse de flux et l'analyse en temps réel. Apache Iceberg est la clé du lac de données ouvert.

Iceberg est compatible avec une variété de moteurs de calcul, y compris Spark. CDE permet de déployer des clusters virtuels compatibles avec Iceberg.

Pour plus d'informations, veuillez consulter la [documentation](https://iceberg.apache.org/).

##### Interface Utilisateur CDE

Maintenant que vous avez couvert les bases de CDE, passez quelques instants à vous familiariser avec la page d'accueil de CDE.

La page d'accueil fournit un aperçu général de tous les services CDE et des clusters. En haut, vous avez des raccourcis pour créer des jobs et des ressources CDE.

![alt text](../../img/new_home_119.png)

Faites défiler vers le bas pour la section des Clusters Virtuels CDE et notez que tous les clusters virtuels et chaque environnement CDP / service CDE associé sont affichés.

![alt text](../../img/new_home_119_2.png)

Ensuite, ouvrez la page d'administration sur l'onglet de gauche. Cette page montre également les services CDE à gauche et les clusters virtuels associés à droite.

![alt text](../../img/service_cde.png)

Ouvrez la page des détails du service CDE et notez les informations et liens clés suivants :

* Version CDE
* Plage d'échelle automatique des nœuds
* Lac de données CDP et environnement
* Graphana Charts. Cliquez sur ce lien pour obtenir un tableau de bord des ressources Kubernetes en cours d'exécution.
* Planificateur de ressources. Cliquez sur ce lien pour voir l'interface Web Yunikorn.

![alt text](../../img/service_cde_2.png)

Faites défiler vers le bas et ouvrez l'onglet Configurations. Notez que c'est ici que sont définis les types d'instance et les plages d'échelle automatique des instances.

![alt text](../../img/cde_configs.png)

Pour en savoir plus sur d'autres configurations importantes du service, veuillez consulter [Activer un service CDE](https://docs.cloudera.com/data-engineering/cloud/enable-data-engineering/topics/cde-enable-data-engineering.html) dans la documentation CDE.

Retournez à la page d'administration et ouvrez la page des détails d'un cluster virtuel.

![alt text](../../img/cde_virtual_cluster_details.png)

Cette vue inclut d'autres informations importantes sur la gestion du cluster. À partir d'ici, vous pouvez :

* Télécharger les binaires de la CLI CDE. La CLI est recommandée pour soumettre des jobs et interagir avec CDE. Elle est couverte dans la partie 3 de ce guide.
* Visiter la documentation API pour découvrir l'API CDE et construire des requêtes d'exemple sur la page Swagger.
* Accéder à l'interface Airflow pour surveiller vos jobs Airflow, configurer des connexions personnalisées, des variables, et plus encore.

Ouvrez l'onglet Configuration. Notez que vous pouvez choisir entre les clusters de niveau Core et All Purpose.
De plus, cette vue permet de définir les plages d'échelle automatique de CPU et de mémoire, la version de Spark, et les options Iceberg.

CDE prend en charge les versions de Spark 3.5.1.

![alt text](../../img/vc_details_1.png)

![alt text](../../img/vc_details_2.png)

![alt text](../../img/vc_details_3.png)

Pour en savoir plus sur l'architecture CDE, veuillez consulter [Créer et gérer des clusters virtuels](https://docs.cloudera.com/data-engineering/cloud/manage-clusters/topics/cde-create-cluster.html) et [Recommandations pour l'optimisation des déploiements CDE](https://docs.cloudera.com/data-engineering/cloud/deployment-architecture/topics/cde-general-scaling.html)

## Résumé

Un service CDE définit les types d'instance de calcul, les plages d'échelle automatique des instances et le lac de données CDP associé. Les données et les utilisateurs associés au service sont soumis aux services SDX et aux paramètres de l'environnement CDP. Vous pouvez utiliser SDX Atlas et Ranger pour visualiser les métadonnées des tables et des jobs, et sécuriser l'accès des utilisateurs et des données avec des politiques détaillées.
