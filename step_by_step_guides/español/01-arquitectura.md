# Arquitectura de CDE

## Objetivo

En esta sección aprenderás sobre la arquitectura flexible de CDE y sus componentes principales.

## Tabla de Contenidos

* [Introducción al Servicio CDE](https://github.com/pdefusco/CDE119_ACE_WORKSHOP/blob/main/step_by_step_guides/spanish/part01_cde_architecture.md#introducci%C3%B3n-al-servicio-cde)
  * [Entorno CDP](https://github.com/pdefusco/CDE119_ACE_WORKSHOP/blob/main/step_by_step_guides/spanish/part01_cde_architecture.md#entorno-cdp)
  * [Servicio CDE](https://github.com/pdefusco/CDE119_ACE_WORKSHOP/blob/main/step_by_step_guides/spanish/part01_cde_architecture.md#servicio-cde)
  * [Cluster Virtual](https://github.com/pdefusco/CDE119_ACE_WORKSHOP/blob/main/step_by_step_guides/spanish/part01_cde_architecture.md#cluster-virtual)
  * [Trabajos de CDE](https://github.com/pdefusco/CDE119_ACE_WORKSHOP/blob/main/step_by_step_guides/spanish/part01_cde_architecture.md#trabajos-de-cde)
  * [Recurso de CDE](https://github.com/pdefusco/CDE119_ACE_WORKSHOP/blob/main/step_by_step_guides/spanish/part01_cde_architecture.md#recurso-de-cde)
  * [Ejecución de Trabajo](https://github.com/pdefusco/CDE119_ACE_WORKSHOP/blob/main/step_by_step_guides/spanish/part01_cde_architecture.md#ejecuci%C3%B3n-de-trabajo)
  * [Sesiones de CDE](https://github.com/pdefusco/CDE119_ACE_WORKSHOP/blob/main/step_by_step_guides/spanish/part01_cde_architecture.md#sesiones-de-cde)
  * [Apache Iceberg](https://github.com/pdefusco/CDE119_ACE_WORKSHOP/blob/main/step_by_step_guides/spanish/part01_cde_architecture.md#apache-iceberg)
  * [Interfaz de Usuario de CDE](https://github.com/pdefusco/CDE119_ACE_WORKSHOP/blob/main/step_by_step_guides/spanish/part01_cde_architecture.md#interfaz-de-usuario-de-cde)
* [Resumen](https://github.com/pdefusco/CDE119_ACE_WORKSHOP/blob/main/step_by_step_guides/spanish/part01_cde_architecture.md#resumen)

## Introducción al Servicio CDE

Cloudera Data Engineering (CDE) es un servicio de Cloudera Data Platform que permite enviar trabajos por lotes a clústeres virtuales con autoescalado. CDE te permite dedicar más tiempo a tus aplicaciones y menos a la infraestructura.

Cloudera Data Engineering permite crear, administrar y programar trabajos de Apache Spark sin la sobrecarga de crear y mantener clústeres Spark. Con Cloudera Data Engineering, defines clústeres virtuales con un rango de recursos de CPU y memoria, y el clúster escala automáticamente según sea necesario para ejecutar tus cargas de trabajo Spark, ayudando a controlar los costos en la nube.

El Servicio CDE se puede acceder desde la página de inicio de CDP haciendo clic en el ícono azul "Data Engineering".

![texto alternativo](../../img/cdp_lp_0.png)

La página de inicio de CDE te permite acceder, crear y gestionar Servicios CDE y Clústeres Virtuales. Dentro de cada Servicio CDE, puedes desplegar uno o más Clústeres Virtuales CDE. En el Clúster Virtual, puedes crear, monitorear y solucionar problemas de trabajos de Spark y Airflow.

El Servicio CDE está vinculado al Entorno CDP. Cada Servicio CDE está mapeado a un máximo de un Entorno CDP, mientras que un Entorno CDP puede estar mapeado a uno o más Servicios CDE.

Estos son los componentes más importantes en el Servicio CDE:

##### Ambiente CDP
Un subconjunto lógico de tu cuenta de proveedor de nube que incluye una red virtual específica. Los Entornos CDP pueden estar en AWS, Azure, RedHat OCP y Cloudera ECS. Para más información, consulta [Entornos CDP](https://docs.cloudera.com/management-console/cloud/overview/topics/mc-core-concepts.html). Prácticamente, un entorno equivale a un Data Lake, ya que cada entorno se asocia automáticamente con sus propios servicios SDX para Seguridad, Gobernanza y Linaje.

##### Servicio CDE
El clúster de Kubernetes de larga duración y los servicios que administran los clústeres virtuales. El servicio CDE debe estar habilitado en un entorno antes de que puedas crear clústeres virtuales.

##### Clúster Virtual
Un clúster de autoescalado individual con rangos predefinidos de CPU y memoria. Los Clústeres Virtuales en CDE pueden ser creados y eliminados bajo demanda. Los trabajos están asociados con los clústeres. Al desplegar un Clúster Virtual, puedes elegir entre dos niveles de clúster:

*Core (Nivel 1)*: Opciones de transformación y ingeniería basadas en lotes incluyen:
* Clúster con autoescalado
* Instancias Spot
* SDX/Lakehouse
* Ciclo de vida del trabajo
* Monitoreo
* Orquestación de flujo de trabajo

*All Purpose (Nivel 2)*: Desarrolla usando sesiones interactivas y despliega cargas de trabajo por lotes y en streaming. Esta opción incluye todas las opciones del Nivel 1 con la adición de lo siguiente:
* Sesiones Shell - CLI y Web
* JDBC/SparkSQL (Disponible en octubre de 2023 con CDE 1.20)
* IDE (Disponible en octubre de 2023 con CDE 1.20)

Los clústeres Core se recomiendan como entornos de Producción. En cambio, los clústeres All Purpose están diseñados para ser utilizados en entornos de Desarrollo y Pruebas.  
Para obtener más información sobre las versiones CDE 1.19.1 y 1.19.2, visita esta página en la [documentación](https://docs.cloudera.com/data-engineering/cloud/release-notes/topics/cde-whats-new-1.19.html).

##### Trabajos (Jobs)  
Código de aplicación junto con configuraciones y recursos definidos. Los trabajos pueden ejecutarse bajo demanda o programarse. Una ejecución individual de un trabajo se denomina "job run".

##### Recurso  
Una colección definida de archivos, como un archivo Python o un JAR de aplicación, dependencias y cualquier otro archivo de referencia necesario para un trabajo.

##### Ejecución de Trabajo (Job Run)  
Ejecución individual de un trabajo.

##### Sesión CDE  

Las sesiones interactivas de CDE proporcionan a los ingenieros de datos puntos de acceso flexibles para comenzar a desarrollar aplicaciones Spark desde cualquier lugar: en un terminal basado en la web, CLI local, IDE favorito e incluso mediante JDBC desde herramientas de terceros.

##### Apache Iceberg  

Apache Iceberg es un formato de tabla abierta nativo en la nube y de alto rendimiento para organizar conjuntos de datos analíticos a escala de petabytes en un sistema de archivos o en un almacenamiento de objetos. Combinado con Cloudera Data Platform (CDP), los usuarios pueden construir una arquitectura de lago de datos abierta (open data lakehouse) para análisis multifuncionales y desplegar grandes canalizaciones de datos de extremo a extremo.

El Open Data Lakehouse en CDP simplifica el análisis avanzado de todos los datos con una plataforma unificada para datos estructurados y no estructurados, junto con servicios de datos integrados que permiten cualquier caso de uso analítico, desde aprendizaje automático (ML) y BI hasta análisis en streaming y en tiempo real. Apache Iceberg es la clave del open lakehouse.

Iceberg es compatible con una variedad de motores de cómputo, incluido Spark. CDE permite implementar Clústeres Virtuales habilitados para Iceberg.

Para obtener más información, visita la [documentación](https://iceberg.apache.org/).

##### Interfaz de Usuario de CDE  

Ahora que has cubierto los conceptos básicos de CDE, dedica unos momentos a familiarizarte con la página de inicio de CDE.

La Página de Inicio proporciona una visión general de alto nivel de todos los Servicios y Clústeres de CDE. En la parte superior, tienes accesos directos para crear Trabajos y Recursos en CDE.  

![alt text](../../img/new_home_119.png)

Desplázate hacia abajo hasta la sección de Clústeres Virtuales de CDE y observa que se muestran todos los Clústeres Virtuales junto con cada Entorno CDP / Servicio CDE asociado.  

![alt text](../../img/new_home_119_2.png)

A continuación, abre la página de Administración en la pestaña de la izquierda. Esta página también muestra los Servicios de CDE a la izquierda y los Clústeres Virtuales asociados a la derecha.  

![alt text](../../img/service_cde.png)

Abre la página de Detalles del Servicio CDE y observa la siguiente información clave y enlaces:  

* Versión de CDE  
* Rango de Autoescalado de Nodos  
* Data Lake y Entorno CDP  
* Gráficos de Grafana. Haz clic en este enlace para obtener un panel de control de los recursos en ejecución de Kubernetes del Servicio.  
* Planificador de Recursos. Haz clic en este enlace para ver la interfaz web de YuniKorn.  

![alt text](../../img/service_cde_2.png)

Desplázate hacia abajo y abre la pestaña de Configuraciones. Aquí se definen los Tipos de Instancia y los rangos de Autoescalado de Instancias.  

![alt text](../../img/cde_configs.png)

Para conocer más sobre otras configuraciones importantes del servicio, visita la [documentación](https://docs.cloudera.com/data-engineering/cloud/enable-data-engineering/topics/cde-enable-data-engineering.html) sobre Habilitación de un Servicio CDE.

Regresa a la página de Administración y abre la página de Detalles del Clúster Virtual.  

![alt text](../../img/cde_virtual_cluster_details.png)

Esta vista incluye otra información clave de administración del clúster. Desde aquí puedes:

* Descargar los binarios del CLI de CDE. Se recomienda la CLI para enviar trabajos e interactuar con CDE. Se cubre en la Parte 3 de esta guía.  
* Visitar la API Docs para aprender sobre la API de CDE y construir solicitudes de ejemplo en la página Swagger.  
* Acceder a la UI de Airflow para monitorear tus trabajos de Airflow, configurar conexiones personalizadas, variables y más.  

Abre la pestaña de Configuración. Observa que puedes seleccionar entre Clústeres de Nivel Core y All Purpose.  
Además, esta vista proporciona opciones para establecer rangos de autoescalado de CPU y Memoria, la versión de Spark y opciones de Iceberg.  
CDE es compatible con Spark 3.5.1.  

![alt text](../../img/vc_details_1.png)

![alt text](../../img/vc_details_2.png)

![alt text](../../img/vc_details_3.png)

Para conocer más sobre la Arquitectura de CDE, visita [Creación y Gestión de Clústeres Virtuales](https://docs.cloudera.com/data-engineering/cloud/manage-clusters/topics/cde-create-cluster.html) y [Recomendaciones para Escalar Implementaciones de CDE](https://docs.cloudera.com/data-engineering/cloud/deployment-architecture/topics/cde-general-scaling.html).

## Resumen  

Un Servicio CDE define los tipos de instancia de cómputo, los rangos de autoescalado de instancias y el Data Lake de CDP asociado. Los datos y los usuarios asociados al servicio están sujetos a SDX y a la configuración del Entorno CDP. Puedes aprovechar SDX Atlas y Ranger para visualizar metadatos de tablas y trabajos, y para asegurar el acceso a usuarios y datos con políticas de seguridad detalladas.

Dentro de un Servicio CDE, puedes implementar uno o más Clústeres Virtuales de CDE.  
El Rango de Autoescalado del Servicio define la cantidad mínima y máxima de instancias de cómputo permitidas.  
El Rango de Autoescalado del Clúster Virtual define la cantidad mínima y máxima de CPU y Memoria que pueden utilizarse para todos los Trabajos en el clúster. Este rango está naturalmente limitado por los recursos de CPU y Memoria disponibles a nivel de Servicio.

CDE es compatible con Spark en sus versiones 3.5.1. Cada Clúster Virtual de CDE se implementa con una única versión de Spark.

Esta arquitectura flexible permite aislar cargas de trabajo y limitar el acceso dentro de diferentes clústeres de cómputo con autoescalado, mientras se predefinen límites de gestión de costos a nivel agregado. Por ejemplo, puedes definir Servicios a nivel organizacional y dentro de ellos, Clústeres Virtuales para entornos de Desarrollo (DEV), Pruebas (QA) y Producción (PROD).

CDE aprovecha la planificación de recursos de YuniKorn y políticas de ordenación, como gang scheduling y bin packing, para optimizar la utilización de recursos y mejorar la eficiencia de costos. Para obtener más información sobre gang scheduling, consulta el blog de Cloudera: [Spark on Kubernetes – Gang Scheduling with YuniKorn](https://blog.cloudera.com/spark-on-kubernetes-gang-scheduling-with-yunikorn/).

El autoescalado de Trabajos Spark en CDE está controlado por la asignación dinámica de Apache Spark. Esta funcionalidad ajusta dinámicamente la cantidad de ejecutores según la necesidad del trabajo en ejecución, lo que permite optimizar el rendimiento y la asignación de recursos.
