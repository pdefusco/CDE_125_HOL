# Sviluppo di Applicazioni Spark in CDE

![alt text](../../img/spark-connect-slide.png)

## Contenuti

1. [Sviluppo di Applicazioni Spark](https://github.com/pdefusco/CDE_124_HOL/blob/main/step_by_step_guides/english/02-development.md#lab-1-spark-application-development).  
2. [Repository CDE, Job e Monitoraggio](https://github.com/pdefusco/CDE_124_HOL/blob/main/step_by_step_guides/english/02-development.md#lab-2-cde-repositories-jobs-and-monitoring).

Prototiperemo e testeremo le operazioni di "Merge Into" e di Lettura Incrementale di Iceberg.

## Lab 1. Sviluppo di Applicazioni Spark

#### Clonare il Container Docker e Avviare l'IDE

Clona il repository GitHub sulla tua macchina locale.

```
git clone https://github.com/pdefusco/CDE_124_HOL.git
cd CDE_124_HOL
```

Avvia il container Docker.

```
docker run -p 8888:8888 pauldefusco/cde_124_hol
```

Avvia l'IDE JupyterLab nel tuo browser copiando e incollando l'URL fornito, come mostrato di seguito.

![alt text](../../img/docker-container-launch.png)

Ora hai accesso a tutti i materiali del laboratorio dal pannello sinistro di JupyterLab. Da qui, puoi avviare i notebook e eseguire il terminale.

![alt text](../../img/jl-home.png)

Utilizzerai il terminale nell'IDE per eseguire i comandi CDE CLI per i laboratori. Prima, però, è necessario configurare la CLI e installare Spark Connect.

#### Configurare la CLI CDE e Installare Spark Connect per CDE

Apri le configurazioni di CDE e applica il tuo nome utente per il Workload e l'URL dell'API dei Job. Puoi trovare l'URL dell'API dei Job nella pagina dei dettagli del tuo Virtual Cluster.

![alt text](../../img/jobs-api-url-1.png)

![alt text](../../img/jobs-api-url-2.png)

![alt text](../../img/cli-configs-1.png)

![alt text](../../img/cli-configs-2.png)

Successivamente, genera un token di accesso CDP ed edita le tue credenziali CDP.

![alt text](../../img/usr-mgt-1.png)

![alt text](../../img/usr-mgt-2.png)

![alt text](../../img/usr-mgt-3.png)

![alt text](../../img/cdp-credentials.png)

Infine, crea un ambiente Python e installa i tarball di CDE Spark Connect.

```
pip3 install cdeconnect.tar.gz  
pip3 install pyspark-3.5.1.tar.gz
```

![alt text](../../img/install-deps.png)

#### Avvia una Sessione CDE Spark Connect

Avvia una Sessione CDE di tipo Spark Connect. Modifica il parametro del nome della sessione in modo che non entri in conflitto con le sessioni di altri utenti. Ti verrà richiesto di inserire la password del tuo Workload. Questa è la stessa password che hai utilizzato per accedere a CDP.

```
cde session create \
  --name paul-hol-session \
  --type spark-connect \
  --num-executors 2 \
  --driver-cores 2 \
  --driver-memory "2g" \
  --executor-cores 2 \
  --executor-memory "2g"
```

![alt text](../../img/launchsess.png)

Nell'interfaccia delle sessioni, verifica che la sessione sia in esecuzione.

![alt text](../../img/cde_session_validate_1.png)

![alt text](../../img/cde_session_validate_2.png)

#### Esegui la tua Prima Applicazione PySpark e Iceberg tramite Spark Connect

Ora sei pronto per connetterti alla Sessione CDE dal tuo IDE JupyterLab locale utilizzando Spark Connect.

Apri Iceberg_TimeTravel_PySpark.ipynb. Aggiorna il nome della sessione Spark Connect, il nome utente e le variabili di Storage Location nelle prime due celle. Quindi esegui ogni cella nel notebook.

```
from cde import CDESparkConnectSession
spark = CDESparkConnectSession.builder.sessionName('<your-spark-connect-session-name-here>').get()
```

```
storageLocation = <your-storage-location-here>
username = <your-cdp-workload-username-here>
```

![alt text](../../img/runnotebook-1.png)

#### Prototipa l'Applicazione Spark & Iceberg come Spark Submit

Nel tuo terminale esegui i seguenti comandi per eseguire il tuo codice come Spark Submit. Assicurati di modificare l'opzione "vcluster-endpoint" in base all'URL dell'API dei Job del tuo Virtual Cluster.

```
cde spark submit \
  pyspark-app.py \
  --vcluster-endpoint <your-DEV-vc-jobs-api-url-here> \
  --executor-memory "4g" \
  --executor-cores 2 \
  <your-storage-location-here> \
  <your-cdp-workload-username-here>
```

Ad esempio:

```
cde spark submit \
  pyspark-app.py \
  --vcluster-endpoint https://9rqklznh.cde-8qhz2284.pdefusco.a465-9q4k.cloudera.site/dex/api/v1 \
  --executor-memory "4g" \
  --executor-cores 2 \
  s3a://cde-hol-buk-d2ab0f50/data/cde-124-hol \
  user001
```

Attendi che l'applicazione venga eseguita e verifica i risultati nel terminale.

![alt text](../../img/cde-spark-submit.png)

![alt text](../../img/cli-submit.png)

Ora sei pronto per convertire lo Spark Submit in un CDE Spark Job.

## Lab 2. Repository CDE, Job e Monitoraggio

I repository CDE vengono utilizzati per importare file e dipendenze nei Virtual Clusters clonando repository Git. Crea il tuo Repository CDE e sincronizzalo con il Repository Git. Assicurati di aggiornare i parametri del nome e del vcluster-endpoint prima di eseguire i comandi CLI.

```
cde repository create --name sparkAppRepoDevUser001 \
  --branch main \
  --url https://github.com/pdefusco/CDE_124_HOL.git \
  --vcluster-endpoint <your-DEV-vc-jobs-api-url-here>

cde repository sync --name sparkAppRepoDevUser001 \
  --vcluster-endpoint <your-DEV-vc-jobs-api-url-here>
```

![alt text](../../img/repos.png)

![alt text](../../img/cde-repos-1.png)

![alt text](../../img/cde-repos-2.png)

#### Distribuisci usando CLI

Ora crea un CDE Spark job utilizzando il Repository CDE come dipendenza.

I file nel Repository vengono montati e sono accessibili dall'Applicazione durante l'esecuzione.

Prima di eseguire i comandi CLI, aggiorna le opzioni del nome, della risorsa e del vcluster endpoint in base al tuo nome utente assegnato.

```
cde job create --name cde_spark_iceberg_job_user001 \
  --type spark \
  --mount-1-resource sparkAppRepoDevUser001 \
  --executor-cores 2 \
  --executor-memory "4g" \
  --application-file pyspark-app.py\
  --vcluster-endpoint <your-DEV-vc-jobs-api-url-here> \
  --arg <your-storage-location-here> \
  --arg <your-cdp-workload-username-here>
```

```
cde job run --name cde_spark_iceberg_job_user001 \
  --executor-cores 4 \
  --executor-memory "2g" \
  --vcluster-endpoint <your-DEV-vc-jobs-api-url-here>
```

![alt text](../../img/cde-job-1.png)

![alt text](../../img/cde-job-2.png)

![alt text](../../img/cde-job-3.png)

![alt text](../../img/cde-job-4.png)

![alt text](../../img/cde-job-5.png)

![alt text](../../img/cde-job-6.png)

#### Monitoraggio

Vai all'interfaccia dei Job Runs / esegui alcuni comandi CDE CLI per verificare lo stato.

```
# Elenca tutti i Job nel Virtual Cluster:
cde job list \
  --vcluster-endpoint <your-DEV-vc-jobs-api-url-here>
```

![alt text](../../img/cde-job-list-1.png)

```
# Elenca tutti i job nel Virtual Cluster il cui nome è "cde_spark_job_user001":
cde job list \
  --filter 'name[eq]cde_spark_iceberg_job_user001' \
  --vcluster-endpoint <your-DEV-vc-jobs-api-url-here>

# Elenca tutti i job nel Virtual Cluster il cui nome del file dell'applicazione del job è "pyspark-app.py":
cde job list \
  --filter 'spark.file

[eq]pyspark-app.py' \
  --vcluster-endpoint <your-DEV-vc-jobs-api-url-here>
```

![alt text](../../img/cde-job-list-2.png)

```
# Elenca tutte le esecuzioni del Job "cde_spark_job_user001":
cde run list \
  --filter 'job[eq]cde_spark_iceberg_job_user001' \
  --vcluster-endpoint <your-DEV-vc-jobs-api-url-here>
```

![alt text](../../img/cde-job-list-3.png)

## Riepilogo e Passi Successivi

Una Spark Connect Session è un tipo di CDE Session che espone l'interfaccia Spark Connect. Una Spark Connect Session ti consente di connetterti a Spark da qualsiasi ambiente Python remoto.

Spark Connect ti consente di connetterti in remoto ai cluster Spark. Spark Connect è un'API che utilizza l'API DataFrame e i piani logici non risolti come protocollo.

In questa sezione dei laboratori abbiamo rivisitato un framework completo per sviluppatori utilizzando Spark Connect, la CDE CLI e Apache Iceberg. Potresti anche trovare rilevanti i seguenti articoli e demo:

* [Installazione della CDE CLI](https://docs.cloudera.com/data-engineering/cloud/cli-access/topics/cde-cli.html)
* [Introduzione semplice alla CDE CLI](https://github.com/pdefusco/CDE_CLI_Simple)
* [Concetti di CDE](https://docs.cloudera.com/data-engineering/cloud/cli-access/topics/cde-cli-concepts.html)
* [Riferimento comandi CDE CLI](https://docs.cloudera.com/data-engineering/cloud/cli-access/topics/cde-cli-reference.html)
* [CDE Spark Connect](https://docs.cloudera.com/data-engineering/cloud/spark-connect-sessions/topics/cde-spark-connect-session.html)
* [Riferimento API dei Job di CDE](https://docs.cloudera.com/data-engineering/cloud/jobs-rest-api-reference/index.html)
* [Utilizzo di Apache Iceberg in CDE](https://docs.cloudera.com/data-engineering/cloud/manage-jobs/topics/cde-using-iceberg.html)
* [Come creare una tabella Apache Iceberg in CDE](https://community.cloudera.com/t5/Community-Articles/How-to-Create-an-Iceberg-Table-with-PySpark-in-Cloudera-Data/ta-p/394800)
