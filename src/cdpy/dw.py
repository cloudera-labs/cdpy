# -*- coding: utf-8 -*-

from cdpy.common import CdpSdkBase, Squelch, CdpError

ENTITLEMENT_DISABLED = "Data Warehousing not enabled on CDP Tenant"


class CdpyDw(CdpSdkBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def list_dbcs(self, cluster_id):
        return self.sdk.call(
            svc="dw",
            func="list_dbcs",
            ret_field="dbcs",
            squelch=[
                Squelch(value="NOT_FOUND", default=list()),
                Squelch(
                    field="status_code",
                    value="504",
                    default=list(),
                    warning="No Data Catalogs found in this Cluster",
                ),
                Squelch(
                    value="PATH_DISABLED", warning=ENTITLEMENT_DISABLED, default=list()
                ),
            ],
            clusterId=cluster_id,
        )

    def list_vws(self, cluster_id):
        return self.sdk.call(
            svc="dw",
            func="list_vws",
            ret_field="vws",
            squelch=[
                Squelch(value="NOT_FOUND", default=list()),
                Squelch(
                    field="status_code",
                    value="504",
                    default=list(),
                    warning="No Virtual Warehouses found in this Cluster",
                ),
                Squelch(
                    value="PATH_DISABLED", warning=ENTITLEMENT_DISABLED, default=list()
                ),
            ],
            clusterId=cluster_id,
        )

    def describe_cluster(self, cluster_id):
        return self.sdk.call(
            svc="dw",
            func="describe_cluster",
            ret_field="cluster",
            squelch=[
                Squelch("NOT_FOUND"),
                Squelch("INVALID_ARGUMENT"),
                Squelch(value="PATH_DISABLED", warning=ENTITLEMENT_DISABLED),
            ],
            clusterId=cluster_id,
        )

    def describe_vw(self, cluster_id, vw_id):
        return self.sdk.call(
            svc="dw",
            func="describe_vw",
            ret_field="vw",
            squelch=[
                Squelch("NOT_FOUND"),
                Squelch("INVALID_ARGUMENT"),
                Squelch("UNKNOWN"),
                Squelch(value="PATH_DISABLED", warning=ENTITLEMENT_DISABLED),
            ],
            clusterId=cluster_id,
            vwId=vw_id,
        )

    def describe_dbc(self, cluster_id, dbc_id):
        return self.sdk.call(
            svc="dw",
            func="describe_dbc",
            ret_field="dbc",
            squelch=[
                Squelch("NOT_FOUND"),
                Squelch("INVALID_ARGUMENT"),
                Squelch("UNKNOWN"),
                Squelch(value="PATH_DISABLED", warning=ENTITLEMENT_DISABLED),
            ],
            clusterId=cluster_id,
            dbcId=dbc_id,
        )

    def describe_data_visualization(self, cluster_id, data_viz_id):
        return self.sdk.call(
            svc="dw",
            func="describe_data_visualization",
            ret_field="dataVisualization",
            squelch=[
                Squelch("NOT_FOUND"),
                Squelch("not found", field="violations"),
                Squelch("INVALID_ARGUMENT"),
                Squelch(value="PATH_DISABLED", warning=ENTITLEMENT_DISABLED),
            ],
            clusterId=cluster_id,
            dataVisualizationId=data_viz_id,
        )

    def list_clusters(self, env_crn=None):
        resp = self.sdk.call(
            svc="dw",
            func="list_clusters",
            ret_field="clusters",
            squelch=[
                Squelch(value="NOT_FOUND", default=list()),
                Squelch(
                    value="PATH_DISABLED", warning=ENTITLEMENT_DISABLED, default=list()
                ),
            ],
        )
        if env_crn:
            return [x for x in resp if env_crn == x["environmentCrn"]]
        return resp

    def list_data_visualizations(self, cluster_id):
        return self.sdk.call(
            svc="dw",
            func="list_data_visualizations",
            ret_field="dataVisualizations",
            squelch=[
                Squelch(value="NOT_FOUND", default=list()),
                Squelch(
                    value="PATH_DISABLED", warning=ENTITLEMENT_DISABLED, default=list()
                ),
            ],
            clusterId=cluster_id,
        )

    def gather_clusters(self, env_crn=None):
        self.sdk.validate_crn(env_crn)
        clusters = self.list_clusters(env_crn=env_crn)
        out = []
        if clusters:
            for base in clusters:
                out.append(
                    {
                        "dbcs": self.list_dbcs(base["id"]),
                        "vws": self.list_vws(base["id"]),
                        **base,
                    }
                )
        return out

    def create_cluster(
        self,
        env_crn: str,
        overlay: bool,
        aws_lb_subnets: list = None,
        aws_worker_subnets: list = None,
        az_subnet: str = None,
        az_enable_az: bool = None,
        az_managed_identity: str = None,
        az_enable_private_aks: bool = None,
        az_enable_private_sql: bool = None,
        az_enable_spot_instances: bool = None,
        az_log_analytics_workspace_id: str = None,
        az_network_outbound_type: str = None,
        az_aks_private_dns_zone: str = None,
        az_compute_instance_types: list = None,
        private_load_balancer: bool = None,
        public_worker_node: bool = None,
        custom_subdomain: str = None,
        database_backup_retention_period: int = None,
        reserved_compute_nodes: int = None,
        reserved_shared_services_nodes: int = None,
        resource_pool: str = None,
        lb_ip_ranges: list = None,
        k8s_ip_ranges: list = None,
        pvc_storage_class: str = None,
        pvc_db_client_cert: str = None,
        pvc_db_client_key: str = None,
        private_cloud_options: dict = None,
    ):
        self.sdk.validate_crn(env_crn)
        if all(x is not None for x in [aws_worker_subnets, aws_lb_subnets]):
            aws_options = dict(
                lbSubnetIds=aws_lb_subnets, workerSubnetIds=aws_worker_subnets
            )
        else:
            aws_options = None
        if all(x is not None for x in [az_subnet, az_enable_az, az_managed_identity]):
            azure_options_all = dict(
                subnetId=az_subnet,
                enableAZ=az_enable_az,
                userAssignedManagedIdentity=az_managed_identity,
                enablePrivateAks=az_enable_private_aks,
                enablePrivateSQL=az_enable_private_sql,
                enableSpotInstances=az_enable_spot_instances,
                logAnalyticsWorkspaceId=az_log_analytics_workspace_id,
                outboundType=az_network_outbound_type,
                privateDNSZoneAKS=az_aks_private_dns_zone,
                computeInstanceTypes=az_compute_instance_types,
            )

            azure_options = {
                k: v for k, v in azure_options_all.items() if v is not None
            }
        else:
            azure_options = None

        if any(
            x is not None
            for x in [pvc_storage_class, pvc_db_client_cert, pvc_db_client_key]
        ):
            private_cloud_options = {
                "storageClass": pvc_storage_class,
                "dbClientCredentials": {
                    "certificate": pvc_db_client_cert,
                    "privateKey": pvc_db_client_key,
                },
            }
            # Remove keys with None values
            private_cloud_options = {
                k: v for k, v in private_cloud_options.items() if v is not None
            }
        else:
            private_cloud_options = {}

        return self.sdk.call(
            svc="dw",
            func="create_cluster",
            ret_field="clusterId",
            environmentCrn=env_crn,
            useOverlayNetwork=overlay,
            usePrivateLoadBalancer=private_load_balancer,
            usePublicWorkerNode=public_worker_node,
            awsOptions=aws_options,
            azureOptions=azure_options,
            customSubdomain=custom_subdomain,
            databaseBackupRetentionPeriod=database_backup_retention_period,
            reservedComputeNodes=reserved_compute_nodes,
            reservedSharedServicesNodes=reserved_shared_services_nodes,
            resourcePool=resource_pool,
            whitelistK8sClusterAccessIpCIDRs=k8s_ip_ranges,
            whitelistWorkloadAccessIpCIDRs=lb_ip_ranges,
            privateCloudOptions=private_cloud_options,
            squelch=[Squelch(value="PATH_DISABLED", warning=ENTITLEMENT_DISABLED)],
        )

    def create_data_visualization(
        self,
        cluster_id: str,
        name: str,
        config: dict = None,
        resource_template: str = None,
        image_version: str = None,
    ):
        return self.sdk.call(
            svc="dw",
            func="create_data_visualization",
            ret_field="dataVisualizationId",
            squelch=[Squelch(value="PATH_DISABLED", warning=ENTITLEMENT_DISABLED)],
            clusterId=cluster_id,
            name=name,
            config=config,
            resourceTemplate=resource_template,
            imageVersion=image_version,
        )

    def delete_cluster(self, cluster_id: str, force: bool = False):
        return self.sdk.call(
            svc="dw",
            func="delete_cluster",
            squelch=[
                Squelch("NOT_FOUND"),
                Squelch(value="PATH_DISABLED", warning=ENTITLEMENT_DISABLED),
            ],
            clusterId=cluster_id,
            force=force,
        )

    def delete_data_visualization(self, cluster_id: str, data_viz_id: str):
        return self.sdk.call(
            svc="dw",
            func="delete_data_visualization",
            squelch=[
                Squelch("NOT_FOUND"),
                Squelch(value="PATH_DISABLED", warning=ENTITLEMENT_DISABLED),
            ],
            clusterId=cluster_id,
            dataVisualizationId=data_viz_id,
        )

    def update_data_visualization(
        self, cluster_id: str, data_viz_id: str, config: dict
    ):
        return self.sdk.call(
            svc="dw",
            func="update_data_visualization",
            squelch=[
                Squelch("NOT_FOUND"),
                Squelch(value="PATH_DISABLED", warning=ENTITLEMENT_DISABLED),
            ],
            clusterId=cluster_id,
            dataVisualizationId=data_viz_id,
            config=config,
        )

    def create_vw(
        self,
        cluster_id: str,
        dbc_id: str,
        vw_type: str,
        name: str,
        tshirt_size: str = None,
        autoscaling_min_cluster: int = None,
        autoscaling_max_cluster: int = None,
        autoscaling_auto_suspend_timeout_seconds: int = None,
        autoscaling_disable_auto_suspend: bool = None,
        autoscaling_hive_desired_free_capacity: int = None,
        autoscaling_hive_scale_wait_time_seconds: int = None,
        autoscaling_impala_scale_down_delay_seconds: int = None,
        autoscaling_impala_scale_up_delay_seconds: int = None,
        autoscaling_pod_config_name: str = None,
        common_configs: dict = None,
        application_configs: dict = None,
        ldap_groups: list = None,
        enable_sso: bool = None,
        tags: dict = None,
        enable_unified_analytics: bool = None,
        enable_platform_jwt_auth: bool = None,
        impala_ha_enable_catalog_high_availability: bool = None,
        impala_ha_enable_shutdown_of_coordinator: bool = None,
        impala_ha_high_availability_mode: str = None,
        impala_ha_num_of_active_coordinators: int = None,
        impala_ha_shutdown_of_coordinator_delay_seconds: int = None,
    ):

        if any(
            x is not None
            for x in [
                autoscaling_min_cluster,
                autoscaling_max_cluster,
                autoscaling_auto_suspend_timeout_seconds,
                autoscaling_disable_auto_suspend,
                autoscaling_hive_desired_free_capacity,
                autoscaling_hive_scale_wait_time_seconds,
                autoscaling_impala_scale_down_delay_seconds,
                autoscaling_impala_scale_up_delay_seconds,
                autoscaling_pod_config_name,
            ]
        ):
            autoscaling_all = dict(
                autoSuspendTimeoutSeconds=autoscaling_auto_suspend_timeout_seconds,
                disableAutoSuspend=autoscaling_disable_auto_suspend,
                hiveDesiredFreeCapacity=autoscaling_hive_desired_free_capacity,
                hiveScaleWaitTimeSeconds=autoscaling_hive_scale_wait_time_seconds,
                impalaScaleDownDelaySeconds=autoscaling_impala_scale_down_delay_seconds,
                impalaScaleUpDelaySeconds=autoscaling_impala_scale_up_delay_seconds,
                podConfigName=autoscaling_pod_config_name,
            )
            if autoscaling_min_cluster is not None and autoscaling_min_cluster != 0:
                autoscaling_all["minClusters"] = autoscaling_min_cluster
            if autoscaling_max_cluster is not None and autoscaling_max_cluster != 0:
                autoscaling_all["maxClusters"] = autoscaling_max_cluster

            autoscaling = {k: v for k, v in autoscaling_all.items() if v is not None}

        else:
            autoscaling = None

        if tags is not None:
            tag_list = []
            for key, value in tags.items():
                tag_list.append({"key": key, "value": value})
        else:
            tag_list = None

        if any(
            x is not None
            for x in [common_configs, application_configs, ldap_groups, enable_sso]
        ):
            config = {}
            if common_configs is not None and common_configs:
                config["commonConfigs"] = common_configs
            if application_configs is not None and application_configs:
                config["applicationConfigs"] = application_configs
            if ldap_groups is not None and ldap_groups:
                config["ldapGroups"] = ldap_groups
            if enable_sso is not None:
                config["enableSSO"] = enable_sso
        else:
            config = None

        if any(
            x is not None
            for x in [
                impala_ha_enable_catalog_high_availability,
                impala_ha_enable_shutdown_of_coordinator,
                impala_ha_high_availability_mode,
                impala_ha_num_of_active_coordinators,
                impala_ha_shutdown_of_coordinator_delay_seconds,
            ]
        ):

            impala_ha_settings_all = dict(
                enableCatalogHighAvailability=impala_ha_enable_catalog_high_availability,
                enableShutdownOfCoordinator=impala_ha_enable_shutdown_of_coordinator,
                highAvailabilityMode=impala_ha_high_availability_mode,
                numOfActiveCoordinators=impala_ha_num_of_active_coordinators,
                shutdownOfCoordinatorDelaySeconds=impala_ha_shutdown_of_coordinator_delay_seconds,
            )

            impala_ha_settings = {
                k: v for k, v in impala_ha_settings_all.items() if v is not None
            }

        else:
            impala_ha_settings = None

        return self.sdk.call(
            svc="dw",
            func="create_vw",
            ret_field="vwId",
            clusterId=cluster_id,
            dbcId=dbc_id,
            vwType=vw_type,
            name=name,
            tShirtSize=tshirt_size,
            autoscaling=autoscaling,
            config=config,
            tags=tag_list,
            enableUnifiedAnalytics=enable_unified_analytics,
            platformJwtAuth=enable_platform_jwt_auth,
            impalaHaSettings=impala_ha_settings,
            squelch=[Squelch(value="PATH_DISABLED", warning=ENTITLEMENT_DISABLED)],
        )

    def delete_vw(self, cluster_id: str, vw_id: str):
        return self.sdk.call(
            svc="dw",
            func="delete_vw",
            squelch=[
                Squelch("NOT_FOUND"),
                Squelch(value="PATH_DISABLED", warning=ENTITLEMENT_DISABLED),
            ],
            clusterId=cluster_id,
            vwId=vw_id,
        )

    def start_vw(self, cluster_id: str, vw_id: str):
        return self.sdk.call(
            svc="dw",
            func="start_vw",
            squelch=[
                Squelch("NOT_FOUND"),
                Squelch(value="PATH_DISABLED", warning=ENTITLEMENT_DISABLED),
            ],
            clusterId=cluster_id,
            vwId=vw_id,
        )

    def pause_vw(self, cluster_id: str, vw_id: str):
        return self.sdk.call(
            svc="dw",
            func="pause_vw",
            squelch=[
                Squelch("NOT_FOUND"),
                Squelch(value="PATH_DISABLED", warning=ENTITLEMENT_DISABLED),
            ],
            clusterId=cluster_id,
            vwId=vw_id,
        )

    def restart_vw(self, cluster_id: str, vw_id: str):
        return self.sdk.call(
            svc="dw",
            func="restart_vw",
            squelch=[
                Squelch("NOT_FOUND"),
                Squelch(value="PATH_DISABLED", warning=ENTITLEMENT_DISABLED),
            ],
            clusterId=cluster_id,
            vwId=vw_id,
        )

    def create_dbc(self, cluster_id: str, name: str, load_demo_data: bool = None):
        return self.sdk.call(
            svc="dw",
            func="create_dbc",
            ret_field="dbcId",
            clusterId=cluster_id,
            name=name,
            loadDemoData=load_demo_data,
            squelch=[Squelch(value="PATH_DISABLED", warning=ENTITLEMENT_DISABLED)],
        )

    def delete_dbc(self, cluster_id: str, dbc_id: str):
        return self.sdk.call(
            svc="dw",
            func="delete_dbc",
            squelch=[
                Squelch("NOT_FOUND"),
                Squelch(value="PATH_DISABLED", warning=ENTITLEMENT_DISABLED),
            ],
            clusterId=cluster_id,
            dbcId=dbc_id,
        )

    def restart_dbc(self, cluster_id: str, dbc_id: str):
        return self.sdk.call(
            svc="dw",
            func="restart_dbc",
            squelch=[
                Squelch("NOT_FOUND"),
                Squelch(value="PATH_DISABLED", warning=ENTITLEMENT_DISABLED),
            ],
            clusterId=cluster_id,
            dbcId=dbc_id,
        )
