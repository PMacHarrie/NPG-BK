/*==============================================================*/
/* DBMS name:      ORACLE Version 11g                           */
/* Created on:     8/12/2013 12:29:25 PM                        */
/*==============================================================*/


alter table ALGOINPUTPROD
   drop constraint FK_ALGOINPU_RELATIONS_PRODUCTD;

alter table ALGOINPUTPROD
   drop constraint FK_ALGOINPU_RELATIONS_ALGORITH;

alter table ALGOOUTPUTPROD
   drop constraint FK_ALGOOUTP_RELATIONS_PRODUCTD;

alter table ALGOOUTPUTPROD
   drop constraint FK_ALGOOUTP_RELATIONS_ALGORITH;

alter table ALGOPARAMETERS
   drop constraint FK_ALGOPARA_RELATIONS_ALGORITH;

alter table ALGOPROCESSINGNODE_XREF
   drop constraint FK_ALGOPROC_ALGOPROCE_PROCESSI;

alter table ALGOPROCESSINGNODE_XREF
   drop constraint FK_ALGOPROC_ALGOPROCE_ALGORITH;

alter table CUSTOMERPOINTSOFCONTACT
   drop constraint FK_CUSTOMER_RELATIONS_POINTOFC;

alter table CUSTOMERPOINTSOFCONTACT
   drop constraint FK_CUSTOMER_RELATIONS_CUSTOMER;

alter table DISPULLJOBCOMPLETIONTIME
   drop constraint FK_DISPULLJ_RELATIONS_DISTRIBU;

alter table DISTRIBUTIONJOB
   drop constraint FK_DISTRIBU_DINODE_DI_DISTRIBU;

alter table DISTRIBUTIONJOB
   drop constraint FK_DISTRIBU_DIREQ_DIJ_DISTRIBU;

alter table DISTRIBUTIONJOBEXTERNALHOST
   drop constraint FK_DISTRIBU_DIJEH_DN__DISTRIBU;

alter table DISTRIBUTIONJOBEXTERNALHOST
   drop constraint FK_DISTRIBU_DJ_DJEH_R_DISTRIBU;

alter table DISTRIBUTIONNODE
   drop constraint FK_DISTRIBU_INHERITAN_RESOURCE;

alter table DISTRIBUTIONNODEJOBBOX
   drop constraint FK_DISTRIBU_DIJBDIJOB_DISJOBCL;

alter table DISTRIBUTIONNODEJOBBOX
   drop constraint FK_DISTRIBU_DIJBDIJOB_DISJOBPR;

alter table DISTRIBUTIONNODEJOBBOX
   drop constraint FK_DISTRIBU_DISTRIBUT_DISTRIBU;

alter table DISTRIBUTIONPREPAREREQUEST
   drop constraint FK_DISTRIBU_DPR_FILEM_FILEMETA;

alter table DISTRIBUTIONREQUEST
   drop constraint FK_DISTRIBU_DIREQDIJO_DISJOBCL;

alter table DISTRIBUTIONREQUEST
   drop constraint FK_DISTRIBU_DIREQDIJO_DISJOBPR;

alter table DISTRIBUTIONREQUEST
   drop constraint FK_DISTRIBU_DR_DPR_XR_DISTRIBU;

alter table DISTRIBUTIONREQUEST
   drop constraint FK_DISTRIBU_SUB_DIREQ_SUBSCRIP;

alter table ENTERPRISEMEASURE
   drop constraint FK_ENTERPRI_FK_EM_EDL_ENTERPRI;

alter table ENTERPRISEORDEREDDIMENSION
   drop constraint FK_ENTERPRI_FK_EOD_ED_ENTERPRI;

alter table ENTERPRISEORDEREDDIMENSION
   drop constraint FK_ENTERPRI_FK_EOD_EN_ENTERPRI;

alter table FILEMETADATA
   drop constraint FK_FILEMETA_RELATIONS_PRODUCTG;

alter table FILEMETADATA
   drop constraint FK_FILEMETA_RELATIONS_PRODUCTD;

alter table FILEQUALITYSUMMARY
   drop constraint FK_FILEQUAL_RELATIONS_PRODUCTQ;

alter table FILEQUALITYSUMMARY
   drop constraint FK_FILEQUAL_RELATIONS_FILEMETA;

alter table FILERETRANSMITCOUNT
   drop constraint FK_FILERETR_RELATIONS_PRODUCTD;

alter table FILERETRYHISTORY
   drop constraint FK_FILERETR_RELATIONS_FILERETR;

alter table HDF5_ARRAY
   drop constraint FK_HDF5_ARR_RELATIONS_HDF5_GRO;

alter table HDF5_ARRAYATTRIBUTE
   drop constraint FK_HDF5_ARR_FK_H5AA_H_HDF5_ARR;

alter table HDF5_DIMENSIONLIST
   drop constraint FK_HDF5_DIM_RELATIONS_HDF5_ARR;

alter table HDF5_GROUP
   drop constraint FK_HDF5_GRO_FK_HDF5_G_HDF5_GRO;

alter table HDF5_GROUP
   drop constraint FK_HDF5_GRO_RELATIONS_HDF5_STR;

alter table HDF5_GROUPATTRIBUTE
   drop constraint FK_HDF5_GRO_RELATIONS_HDF5_GRO;

alter table HDF5_STRUCTURE
   drop constraint FK_HDF5_STR_RELATIONS_PRODUCTD;

alter table INGESTPROCESSSTEP
   drop constraint FK_INGESTPR_RELATIONS_PRODUCTD;

alter table INGESTPROCESSSTEP
   drop constraint FK_INGESTPR_RELATIONS_NDE_SUPP;

alter table INGESTREQUESTBUFFER
   drop constraint FK_INGESTRE_RELATIONS_INGESTRE;

alter table INGESTREQUESTLOG
   drop constraint FK_INGESTRE_RELATIONS_PRODUCTD;

alter table INGESTREQUESTLOG
   drop constraint FK_INGESTRE_RELATIONS_INGESTIN;

alter table JOBSPECINPUT
   drop constraint FK_JOBSPECI_RELATIONS_PRODUCTI;

alter table JOBSPECINPUT
   drop constraint FK_JOBSPECI_RELATIONS_PRINPUTP;

alter table JOBSPECINPUT
   drop constraint FK_JOBSPECI_RELATIONS_FILEMETA;

alter table JOBSPECOUTPUT
   drop constraint FK_JOBSPECO_RELATIONS_PRODUCTI;

alter table JOBSPECOUTPUT
   drop constraint FK_JOBSPECO_RELATIONS_PROUTPUT;

alter table JOBSPECPARAMETERS
   drop constraint FK_JOBSPECP_RELATIONS_PRODUCTI;

alter table JOBSPECPARAMETERS
   drop constraint FK_JOBSPECP_RELATIONS_PRPARAME;

alter table MEASURE_H_ARRAY_XREF
   drop constraint FK_MEASURE__FK_H_ARRA_HDF5_ARR;

alter table MEASURE_H_ARRAY_XREF
   drop constraint FK_MEASURE__FK_MHAX_M_ENTERPRI;

alter table MEASURE_N_ARRAY_XREF
   drop constraint FK_MEASURE__FK_MNAX_M_ENTERPRI;

alter table MEASURE_N_ARRAY_XREF
   drop constraint FK_MEASURE__RELATIONS_NC4_ARRA;

alter table METEOPOINTOBS
   drop constraint FK_METEOPOI_RELATIONS_METEOSTA;

alter table METEOPOINTOBS
   drop constraint FK_METEOPOI_RELATIONS_METEOOBS;

alter table MHA_SUBSET
   drop constraint FK_MHA_SUBS_RELATIONS_MEASURE_;

alter table MNA_SUBSET
   drop constraint FK_MNA_SUBS_RELATIONS_MEASURE_;

alter table NC4_ARRAY
   drop constraint FK_NC4_ARRA_FK_NA_NG_NC4_GROU;

alter table NC4_ARRAYATTRIBUTE
   drop constraint FK_NC4_ARRA_FK_N4AA_N_NC4_ARRA;

alter table NC4_DIMENSION
   drop constraint FK_NC4_DIME_FK_N4D_N4_NC4_GROU;

alter table NC4_DIMENSIONLIST
   drop constraint FK_NC4_DIME_FK_N4_DIM_NC4_DIME;

alter table NC4_DIMENSIONLIST
   drop constraint FK_NC4_DIME_FK_NDL_NA_NC4_ARRA;

alter table NC4_GROUP
   drop constraint FK_NC4_GROU_FK_N4G_N4_NETCDF4_;

alter table NC4_GROUP
   drop constraint FK_NC4_GROU_FK_NETCDF_NC4_GROU;

alter table NC4_GROUPATTRIBUTE
   drop constraint FK_NC4_GROU_FK_NGA_NG_NC4_GROU;

alter table NDE_OPERATORLOG
   drop constraint FK_NDE_OPER_RELATIONS_NDE_USER;

alter table NETCDF4_STRUCTURE
   drop constraint FK_NETCDF4__RELATIONS_PRODUCTD;

alter table NOTIFICATIONREQUEST
   drop constraint FK_NOTIFICA_DIJOB_NOR_DISTRIBU;

alter table NOTIFICATIONREQUEST
   drop constraint FK_NOTIFICA_NOJOB_NOR_NOTIFICA;

alter table PLATFORMACQUISITIONSCHEDULE
   drop constraint FK_PLATFORM_PLATFROM__PLATFORM;

alter table PLATFORMORBIT
   drop constraint FK_PLATFORM_RELATIONS_PLATFORM;

alter table PLATFORMORBITTYPE
   drop constraint FK_PLATFORM_PLATFORMP_PLATFORM;

alter table PLATFORMSENSOR
   drop constraint FK_PLATFORM_PLATFORMS_PLATFORM;

alter table PRINPUTPRODUCT
   drop constraint FK_PRINPUTP_RELATIONS_PRINPUTS;

alter table PRINPUTPRODUCT
   drop constraint FK_PRINPUTP_RELATIONS_ALGOINPU;

alter table PRINPUTSPEC
   drop constraint FK_PRINPUTS_RELATIONS_PRODUCTI;

alter table PROCESSINGNODE
   drop constraint FK_PROCESSI_INHERITAN_RESOURCE;

alter table PROCESSINGNODEJOBBOX
   drop constraint FK_PROCESSI_RELATIONS_PROCESSI;

alter table PROCESSINGNODEJOBBOX
   drop constraint FK_PROCESSI_RELATIONS_JOBCLASS;

alter table PROCESSINGNODEJOBBOX
   drop constraint FK_PROCESSI_RELATIONS_JOBPRIOR;

alter table PRODUCTDATASOURCE
   drop constraint FK_PRODUCTD_PROD_PROD_PRODUCTD;

alter table PRODUCTDATASOURCE
   drop constraint FK_PRODUCTD_RELATIONS_EXTERNAL;

alter table PRODUCTDATASOURCEFILECACHE
   drop constraint FK_PRODUCTD_RELATIONS_PRODUCTD;

alter table PRODUCTDESCRIPTION
   drop constraint FK_PRODUCTD_PRODUCTGE_PRODUCTD;

alter table PRODUCTDESCRIPTION
   drop constraint FK_PRODUCTD_RELATIONS_PRODUCTG;

alter table PRODUCTDESCRIPTION
   drop constraint FK_PRODUCTD_RELATIONS_INGESTIN;

alter table PRODUCTGROUPDATAPARTITION
   drop constraint FK_PRODUCTG_RELATIONS_PRODUCTG;

alter table PRODUCTIONJOB
   drop constraint FK_PRODUCTI_PRODJOBSP_PRODUCTI;

alter table PRODUCTIONJOB
   drop constraint FK_PRODUCTI_RELATIONS_PROCESSI;

alter table PRODUCTIONJOBLOGMESSAGES
   drop constraint FK_PRODUCTI_PRODJOBLO_PRODUCTI;

alter table PRODUCTIONJOBOUTPUTFILES
   drop constraint FK_PRODUCTI_RELATIONS_PRODUCTI;

alter table PRODUCTIONJOBSPEC
   drop constraint FK_PRODUCTI_PRODJOBPR_JOBPRIOR;

alter table PRODUCTIONJOBSPEC
   drop constraint FK_PRODUCTI_PRODJOBSI_JOBCLASS;

alter table PRODUCTIONJOBSPEC
   drop constraint FK_PRODUCTI_PR_PRJOBS_PRODUCTI;

alter table PRODUCTIONJOBSPEC
   drop constraint FK_PRODUCTI_RELATIONS_WEATHERE;

alter table PRODUCTIONRULE
   drop constraint FK_PRODUCTI_PRODRULEJ_JOBCLASS;

alter table PRODUCTIONRULE
   drop constraint FK_PRODUCTI_PRODRULEP_JOBPRIOR;

alter table PRODUCTIONRULE
   drop constraint FK_PRODUCTI_PRODUCTIO_WEATHERE;

alter table PRODUCTIONRULE
   drop constraint FK_PRODUCTI_RELATIONS_ALGORITH;

alter table PRODUCTIONRULE
   drop constraint FK_PRODUCTI_RELATIONS_PLATFORM;

alter table PRODUCTIONRULE
   drop constraint FK_PRODUCTI_RELATIONS_GAZETTEE;

alter table PRODUCTPLATFORM_XREF
   drop constraint FK_PRODUCTP_PRODUCTPL_PLATFORM;

alter table PRODUCTPLATFORM_XREF
   drop constraint FK_PRODUCTP_PRODUCTPL_PRODUCTD;

alter table PRODUCTQUALITYSUMMARY
   drop constraint FK_PRODUCTQ_RELATIONS_PRODUCTD;

alter table PROUTPUTSPEC
   drop constraint FK_PROUTPUT_RELATIONS_PRODUCTI;

alter table PROUTPUTSPEC
   drop constraint FK_PROUTPUT_RELATIONS_ALGOOUTP;

alter table PRPARAMETER
   drop constraint FK_PRPARAME_RELATIONS_PRODUCTI;

alter table PRPARAMETER
   drop constraint FK_PRPARAME_RELATIONS_ALGOPARA;

alter table RESOURCELOGSUMMARY
   drop constraint FK_RESOURCE_RM_RSL_RE_RESOURCE;

alter table RESOURCEMEASUREMENT
   drop constraint FK_RESOURCE_RMON_RMEA_RESOURCE;

alter table RESOURCEMONITOR
   drop constraint FK_RESOURCE_R_RM_REF_RESOURCE;

alter table RESOURCEMONITORALERT
   drop constraint FK_RESOURCE_RM_RMA_RE_RESOURCE;

alter table RESOURCEPLATFORM
   drop constraint FK_RESOURCE_RP_R_REF_RESOURCE;

alter table RESOURCESERVER
   drop constraint FK_RESOURCE_RP_RS_REF_RESOURCE;

alter table RESOURCESERVER
   drop constraint FK_RESOURCE_RP_R_REF2_RESOURCE;

alter table RESOURCESERVICE
   drop constraint FK_RESOURCE_RP_R_REF3_RESOURCE;

alter table RESOURCESERVICE
   drop constraint FK_RESOURCE_RSERVER_R_RESOURCE;

alter table SUBAPPROVALLOG
   drop constraint FK_SUBAPPRO_RELATIONS_SUBSCRIP;

alter table SUBAPPROVALLOG
   drop constraint FK_SUBAPPRO_RELATIONS_SUBAPPRO;

alter table SUBNOTIFICATIONTYPE
   drop constraint FK_SUBNOTIF_RELATIONS_DELIVERY;

alter table SUBNOTIFICATIONTYPE
   drop constraint FK_SUBNOTIF_RELATIONS_SUBSCRIP;

alter table SUBNOTIFICATIONTYPE
   drop constraint FK_SUBNOTIF_RELATIONS_EXTERNAL;

alter table SUBSCRIPTION
   drop constraint FK_SUBSCRIP_RELATIONS_EXTERNAL;

alter table SUBSCRIPTION
   drop constraint FK_SUBSCRIP_RELATIONS_DISJOBCL;

alter table SUBSCRIPTION
   drop constraint FK_SUBSCRIP_RELATIONS_DISJOBPR;

alter table SUBSCRIPTION
   drop constraint FK_SUBSCRIP_RELATIONS_PRODUCTD;

alter table SUBSCRIPTION
   drop constraint FK_SUBSCRIP_RELATIONS_CUSTOMER;

alter table SYSTEMNOTICE
   drop constraint FK_SYSTEMNO_NDE_USER__NDE_USER;

alter table WEATHEREVENT
   drop constraint FK_WEATHERE_RELATIONS_METEOPOI;

alter table WEATHEREVENT
   drop constraint FK_WEATHERE_RELATIONS_WEATHERE;

alter table WEATHEREVENTDEFINITION
   drop constraint FK_WEATHERE_WED_MOV_B_METEOOBS;

drop index RELATIONSHIP_55_FK;

drop index RELATIONSHIP_2_FK;

drop table ALGOINPUTPROD;

drop index RELATIONSHIP_56_FK;

drop index RELATIONSHIP_21_FK;

drop table ALGOOUTPUTPROD;

drop index RELATIONSHIP_58_FK;

drop table ALGOPARAMETERS;

drop index ALGOPROCESSINGNODE_XREF2_FK;

drop index ALGOPROCESSINGNODE_XREF_FK;

drop table ALGOPROCESSINGNODE_XREF;

drop table ALGORITHM cascade;

drop table ARCHIVENOTIFYCONFIG cascade;

drop table CONFIGURATIONREGISTRY cascade;

drop index RELATIONSHIP_29_FK;

drop index RELATIONSHIP_28_FK;

drop table CUSTOMERPOINTSOFCONTACT cascade;

drop table CUSTOMERPROFILE cascade;

drop table DATADENIALFLAG cascade;

drop table DELIVERYNOTIFICATIONTYPE cascade;

drop table DISJOBCLASSCODE cascade;

drop table DISJOBPRIORITYCODE cascade;

drop index RELATIONSHIP_130_FK;

drop table DISPULLJOBCOMPLETIONTIME cascade;

drop table DISTPREPAREREQUESTLOCK cascade;

drop index DJ_FILENAME;

drop index DJ_STATUS_ENQTIME;

drop index DINODE_DIJOB_REF_FK;

drop index DIREQ_DIJOB_REF_FK;

drop table DISTRIBUTIONJOB cascade;

drop index DJ_DJEH_REF_FK;

drop index DIJEH_DN_REF_FK;

drop table DISTRIBUTIONJOBEXTERNALHOST cascade;

drop table DISTRIBUTIONJOBLOCK cascade;

drop table DISTRIBUTIONNODE cascade;

drop index DIJBDIJOBCLASS_REF_FK;

drop index DIJBDIJOBPR_REF_FK;

drop index DISTRIBUTIONNODEDNJB_REF_FK;

drop table DISTRIBUTIONNODEJOBBOX cascade;

drop table DISTRIBUTIONNOTIFYRETRIEVEJOB cascade;

drop index DPR_STATUS_NQTIME;

drop index DPR_FILEMETADATA_REF_FK;

drop table DISTRIBUTIONPREPAREREQUEST cascade;

drop index DIREQDIJOBCLASSCODE_REF_FK;

drop index DIREQDIJOBPRCODE_REF_FK;

drop index DR_DPR_XREF_FK;

drop index SUB_DIREQ_REF_FK;

drop table DISTRIBUTIONREQUEST cascade;

drop table DISTRIBUTIONRETREIVEJOBLOCK cascade;

drop table ENTERPRISEDIMENSION cascade;

drop table ENTERPRISEDIMENSIONLIST cascade;

drop index FK_EM_EDL_FK;

drop table ENTERPRISEMEASURE cascade;

drop index FK_EOD_EDL_FK;

drop index FK_EOD_ENTERPRISEDIM_FK;

drop table ENTERPRISEORDEREDDIMENSION cascade;

drop table EXTERNALDATAHOST cascade;

drop index FM_INSERTTIME;

drop index RELATIONSHIP_106_FK;

drop index RELATIONSHIP_9_FK;

drop index FM_PRODENDTIME;

drop table FILEMETADATA cascade;

drop index RELATIONSHIP_8_FK;

drop index RELATIONSHIP_7_FK;

drop table FILEQUALITYSUMMARY cascade;

drop index RELATIONSHIP_5_FK;

drop table FILERETRANSMITCOUNT cascade;

drop table FILERETRYHISTORY cascade;

drop table GAZETTEER cascade;

drop index RELATIONSHIP_33_FK;

drop table HDF5_ARRAY cascade;

drop index FK_H5AA_H5A_FK;

drop table HDF5_ARRAYATTRIBUTE cascade;

drop index RELATIONSHIP_3_FK;

drop table HDF5_DIMENSIONLIST cascade;

drop index FK_HDF5_GROUP_SELF_FK;

drop index RELATIONSHIP_25_FK;

drop table HDF5_GROUP cascade;

drop index RELATIONSHIP_51_FK;

drop table HDF5_GROUPATTRIBUTE cascade;

drop table HDF5_STRUCTURE cascade;

drop table INGESTINCOMINGDIRECTORY cascade;

drop index RELATIONSHIP_77_FK;

drop index RELATIONSHIP_66_FK;

drop table INGESTPROCESSSTEP cascade;

drop table INGESTREQUESTBUFFER cascade;

drop index IRL_STATUS_CREATETIME; /*OBE, but leaving the drop in */

drop index IRL_ENQ_DETECT_IX; /* New: Feb. 23, 2016 PM */

drop index IRL_STATUS_OBSTIME;

drop index IRL_COMPLETIONTIME;

drop index RELATIONSHIP_124_FK;

drop index RELATIONSHIP_126_FK;

drop table INGESTREQUESTLOG cascade;

drop table INGESTTHROTTLELOCK cascade;

drop table JOBCLASSCODE cascade;

drop table JOBPRIORITYCODE cascade;

drop index RELATIONSHIP_74_FK;

drop index RELATIONSHIP_75_FK;

drop index RELATIONSHIP_45_FK;

drop table JOBSPECINPUT cascade;

drop index RELATIONSHIP_48_FK;

drop index RELATIONSHIP_44_FK;

drop table JOBSPECOUTPUT cascade;

drop index RELATIONSHIP_46_FK;

drop index RELATIONSHIP_43_FK;

drop table JOBSPECPARAMETERS cascade;

drop index FK_H_ARRAY_XREF_MHA_FK;

drop index FK_MHAX_M_FK;

drop table MEASURE_H_ARRAY_XREF cascade;

drop index FK_MNAX_M_FK;

drop index RELATIONSHIP_38_FK;

drop table MEASURE_N_ARRAY_XREF cascade;

drop table METEOOBSVARIABLES cascade;

drop index RELATIONSHIP_62_FK;

drop index RELATIONSHIP_61_FK;

drop table METEOPOINTOBS cascade;

drop table METEOSTATION cascade;

drop index RELATIONSHIP_36_FK;

drop table MHA_SUBSET cascade;

drop index RELATIONSHIP_71_FK;

drop table MNA_SUBSET cascade;

drop index FK_NA_NG_FK;

drop table NC4_ARRAY cascade;

drop index FK_N4AA_N4A_FK;

drop table NC4_ARRAYATTRIBUTE cascade;

drop index FK_N4D_N4G_FK;

drop table NC4_DIMENSION cascade;

drop index FK_N4_DIML_N4_DIM_FK;

drop index FK_NDL_NA_FK;

drop table NC4_DIMENSIONLIST cascade;

drop index FK_N4G_N4S_FK;

drop index FK_NETCDF4_GROUP_SELF_FK;

drop table NC4_GROUP cascade;

drop index FK_NGA_NG_FK;

drop table NC4_GROUPATTRIBUTE cascade;

drop index RELATIONSHIP_129_FK;

drop table NDE_OPERATORLOG cascade;

drop table NDE_SUPPORTFUNCTION cascade;

drop index NDE_USER_UNIQUE;

drop table NDE_USER cascade;

drop table NETCDF4_STRUCTURE cascade;

drop table NOTIFICATIONJOB cascade;

drop index DIJOB_NOREQ_REF_FK;

drop index NOJOB_NOREQ_REF_FK;

drop table NOTIFICATIONREQUEST cascade;

drop table NOTIFYBYFILELOCK cascade;

drop table NOTIFYBYHOURLOCK cascade;

drop table NOTIFYCLASSMANIFESTLOCK cascade;

drop table NOTIFICATIONRETRIEVEJOBLOCK cascade;

drop table PLATFORM cascade;

drop index RELATIONSHIP_125_FK;

drop table PLATFORMACQUISITIONSCHEDULE cascade;

drop index RELATIONSHIP_79_FK;

drop table PLATFORMORBIT cascade;

drop index PLATFORMPLATFORMORBIT_FK_FK;

drop table PLATFORMORBITTYPE cascade;

drop index PLATFORMSENSORPLATFORM_FK_FK;

drop table PLATFORMSENSOR cascade;

drop table POINTOFCONTACT cascade;

drop index RELATIONSHIP_85_FK;

drop index RELATIONSHIP_84_FK;

drop table PRINPUTPRODUCT cascade;

drop index RELATIONSHIP_39_FK;

drop table PRINPUTSPEC cascade;

drop table PROCESSINGNODE cascade;

drop index RELATIONSHIP_120_FK;

drop index RELATIONSHIP_82_FK;

drop index RELATIONSHIP_81_FK;

drop table PROCESSINGNODEJOBBOX cascade;

drop index RELATIONSHIP_72_FK;

drop index PROD_PRODDSOURCE_REF_FK;

drop table PRODUCTDATASOURCE cascade;

drop index RELATIONSHIP_78_FK;

drop table PRODUCTDATASOURCEFILECACHE cascade;

drop index RELATIONSHIP_107_FK;

drop index PRODUCTGEOLOCATIONID_FK;

drop index RELATIONSHIP_76_FK;

drop table PRODUCTDESCRIPTION cascade;

drop table PRODUCTGROUP cascade;

drop index RELATIONSHIP_105_FK;

drop table PRODUCTGROUPDATAPARTITION cascade;

drop index PJ_STATUS_ENQTIME;

drop index RELATIONSHIP_122_FK;

drop index PRODJOBSPECPRODJOB_FK_FK;

drop table PRODUCTIONJOB cascade;

drop table PRODUCTIONJOBLOCK cascade;

drop index PRODJOBLOGMESSAGES_FK_FK;

drop table PRODUCTIONJOBLOGMESSAGES cascade;

drop index RELATIONSHIP_127_FK;

drop table PRODUCTIONJOBOUTPUTFILES cascade;

drop index PJS_STATUS_OBSENDTIME;

drop index PRODJOBPRIORITY_FK;

drop index PRODJOBSIZE_FK;

drop index RELATIONSHIP_69_FK;

drop index PR_PRJOBSPEC_REF_FK;

drop table PRODUCTIONJOBSPEC cascade;

drop table PRODUCTIONJOBSPECLOCK cascade;

drop table PRODUCTIONJOBSPECCRLOCK cascade;

drop index RELATIONSHIP_80_FK;

drop index RELATIONSHIP_83_FK;

drop index PRODRULEJOBSIZE_FK;

drop index PRODRULEPRIORITY_FK;

drop index PRODUCTIONRULEWEATHEREVENT_FK;

drop index RELATIONSHIP_24_FK;

drop table PRODUCTIONRULE cascade;

drop index PRODUCTPLATFORM_XREF2_FK;

drop index PRODUCTPLATFORM_XREF_FK;

drop table PRODUCTPLATFORM_XREF cascade;

drop index RELATIONSHIP_1_FK;

drop table PRODUCTQUALITYSUMMARY cascade;

drop index RELATIONSHIP_57_FK;

drop index RELATIONSHIP_41_FK;

drop table PROUTPUTSPEC cascade;

drop index RELATIONSHIP_59_FK;

drop index RELATIONSHIP_37_FK;

drop table PRPARAMETER cascade;

drop table RESOURCEINSTANCE cascade;

drop index RM_RSL_REF_FK;

drop table RESOURCELOGSUMMARY cascade;

drop index RMON_RMEAS_REF_FK;

drop table RESOURCEMEASUREMENT cascade;

drop index R_RM_REF_FK;

drop table RESOURCEMONITOR cascade;

drop index RM_RMA_REF_FK;

drop table RESOURCEMONITORALERT cascade;

drop index RP_R_REF_FK;

drop table RESOURCEPLATFORM cascade;

drop index RP_R_REF2_FK;

drop index RP_RS_REF_FK;

drop table RESOURCESERVER cascade;

drop index RP_R_REF3_FK;

drop index RSERVER_RSERVICE_REF_FK;

drop table RESOURCESERVICE cascade;

drop table SUBAPPROVALEVENTS cascade;

drop index RELATIONSHIP_20_FK;

drop index RELATIONSHIP_19_FK;

drop table SUBAPPROVALLOG cascade;

drop index RELATIONSHIP_32_FK;

drop index RELATIONSHIP_16_FK;

drop index RELATIONSHIP_15_FK;

drop table SUBNOTIFICATIONTYPE cascade;

drop index RELATIONSHIP_118_FK;

drop index RELATIONSHIP_117_FK;

drop index RELATIONSHIP_114_FK;

drop index RELATIONSHIP_14_FK;

drop index RELATIONSHIP_13_FK;

drop table SUBSCRIPTION cascade;

drop index NDE_USER_SYSNOTICE_FK;

drop table SYSTEMNOTICE cascade;

drop index RELATIONSHIP_64_FK;

drop index RELATIONSHIP_63_FK;

drop table WEATHEREVENT cascade;

drop index WED_MOV_BMC_REF_FK;

/* New indexes to drop, 3/25/2016pgm */
drop index CR_CPN_IDX;

drop index PJS_CS_ST_PPJ_PR_IDX;

drop index PJS_PRID_ST;

drop index PIP_PRIS_PROD_IDX;

/* 3/25/2016pgm */

drop table WEATHEREVENTDEFINITION cascade;

drop sequence S_ALGOPARAMETERS;

drop sequence S_ALGORITHM;

drop sequence S_ARCHIVENOTIFYCONFIG;

drop sequence S_CONFIGURATIONREGISTRY;

drop sequence S_CUSTOMERPROFILE;

drop sequence S_DELIVERYNOTIFICATIONTYPE;

drop sequence S_DISJOBCLASSCODE;

drop sequence S_DISJOBPRIORITYCODE;

drop sequence S_DISTRIBUTIONJOB;

drop sequence S_DISTRIBUTIONNODEJOBBOX;

drop sequence S_DISTRIBUTIONPREPAREREQUEST;

drop sequence S_DISTRIBUTIONREQUEST;

drop sequence S_ENTERPRISEDIMENSION;

drop sequence S_ENTERPRISEDIMENSIONLIST;

drop sequence S_ENTERPRISEMEASURE;

drop sequence S_EXTERNALDATAHOST;

drop sequence S_FILEMETADATA;

drop sequence S_GAZETTEER;

drop sequence S_HDF5_ARRAY;

drop sequence S_HDF5_ARRAYATTRIBUTE;

drop sequence S_HDF5_GROUP;

drop sequence S_HDF5_GROUPATTRIBUTE;

drop sequence S_INGESTINCOMINGDIRECTORY;

drop sequence S_INGESTREQUESTLOG;

drop sequence S_JOBSPECINPUT;

drop sequence S_JOBSPECOUTPUT;

drop sequence S_JOBSPECPARAMETERS;

drop sequence S_METEOPOINTOBS;

drop sequence S_NC4_ARRAY;

drop sequence S_NC4_ARRAYATTRIBUTE;

drop sequence S_NC4_DIMENSION;

drop sequence S_NC4_GROUP;

drop sequence S_NC4_GROUPATTRIBUTE;

drop sequence S_NDE_OPERATORLOG;

drop sequence S_NDE_SUPPORTFUNCTION;

drop sequence S_NDE_USER;

drop sequence S_NOTIFICATIONJOB;

drop sequence S_NOTIFICATIONREQUEST;

drop sequence S_PLATFORM;

drop sequence S_PLATFORMORBITTYPE;

drop sequence S_PLATFORMSENSOR;

drop sequence S_POINTOFCONTACT;

drop sequence S_PRINPUTPRODUCT;

drop sequence S_PRINPUTSPEC;

drop sequence S_PROCESSINGNODEJOBBOX;

drop sequence S_PRODUCTDESCRIPTION;

drop sequence S_PRODUCTGROUP;

drop sequence S_PRODUCTGROUPDATAPARTITION;

drop sequence S_PRODUCTIONJOB;

drop sequence S_PRODUCTIONJOBSPEC;

drop sequence S_PRODUCTIONRULE;

drop sequence S_PROUTPUTSPEC;

drop sequence S_PRPARAMETER;

drop sequence S_RESOURCEINSTANCE;

drop sequence S_RESOURCEMONITOR;

drop sequence S_RESOURCEMONITORALERT;

drop sequence S_RESOURCEPLATFORM;

drop sequence S_RESOURCESERVER;

drop sequence S_RESOURCESERVICE;

drop sequence S_SUBAPPROVALEVENTS;

drop sequence S_SUBSCRIPTION;

drop sequence S_SYSTEMNOTICE;

drop sequence S_WEATHEREVENT;

drop sequence S_WEATHEREVENTDEFINITION;

create sequence S_ALGOPARAMETERS;

create sequence S_ALGORITHM;

create sequence S_ARCHIVENOTIFYCONFIG;

create sequence S_CONFIGURATIONREGISTRY;

create sequence S_CUSTOMERPROFILE;

create sequence S_DELIVERYNOTIFICATIONTYPE;

create sequence S_DISJOBCLASSCODE;

create sequence S_DISJOBPRIORITYCODE;

create sequence S_DISTRIBUTIONJOB;

create sequence S_DISTRIBUTIONNODEJOBBOX;

create sequence S_DISTRIBUTIONPREPAREREQUEST;

create sequence S_DISTRIBUTIONREQUEST;

create sequence S_ENTERPRISEDIMENSION;

create sequence S_ENTERPRISEDIMENSIONLIST;

create sequence S_ENTERPRISEMEASURE;

create sequence S_EXTERNALDATAHOST;

create sequence S_FILEMETADATA;

create sequence S_GAZETTEER;

create sequence S_HDF5_ARRAY;

create sequence S_HDF5_ARRAYATTRIBUTE;

create sequence S_HDF5_GROUP;

create sequence S_HDF5_GROUPATTRIBUTE;

create sequence S_INGESTINCOMINGDIRECTORY;

create sequence S_INGESTREQUESTLOG;

create sequence S_JOBSPECINPUT;

create sequence S_JOBSPECOUTPUT;

create sequence S_JOBSPECPARAMETERS;

create sequence S_METEOPOINTOBS;

create sequence S_NC4_ARRAY;

create sequence S_NC4_ARRAYATTRIBUTE;

create sequence S_NC4_DIMENSION;

create sequence S_NC4_GROUP;

create sequence S_NC4_GROUPATTRIBUTE;

create sequence S_NDE_OPERATORLOG;

create sequence S_NDE_SUPPORTFUNCTION;

create sequence S_NDE_USER;

create sequence S_NOTIFICATIONJOB;

create sequence S_NOTIFICATIONREQUEST;

create sequence S_PLATFORM;

create sequence S_PLATFORMORBITTYPE;

create sequence S_PLATFORMSENSOR;

create sequence S_POINTOFCONTACT;

create sequence S_PRINPUTPRODUCT;

create sequence S_PRINPUTSPEC;

create sequence S_PROCESSINGNODEJOBBOX;

create sequence S_PRODUCTDESCRIPTION CACHE 1;

create sequence S_PRODUCTGROUP;

create sequence S_PRODUCTGROUPDATAPARTITION;

create sequence S_PRODUCTIONJOB;

create sequence S_PRODUCTIONJOBSPEC;

create sequence S_PRODUCTIONRULE;

create sequence S_PROUTPUTSPEC;

create sequence S_PRPARAMETER;

create sequence S_RESOURCEINSTANCE;

create sequence S_RESOURCEMONITOR;

create sequence S_RESOURCEMONITORALERT;

create sequence S_RESOURCEPLATFORM;

create sequence S_RESOURCESERVER;

create sequence S_RESOURCESERVICE;

create sequence S_SUBAPPROVALEVENTS;

create sequence S_SUBSCRIPTION;

create sequence S_SYSTEMNOTICE;

create sequence S_WEATHEREVENT;

create sequence S_WEATHEREVENTDEFINITION;

/*==============================================================*/
/* Table: ALGOINPUTPROD                                         */
/*==============================================================*/
create table ALGOINPUTPROD
(
   PRODUCTID            BIGINT           not null,
   ALGORITHMID          BIGINT           not null,
   constraint PK_ALGOINPUTPROD primary key (PRODUCTID, ALGORITHMID)
);

/*==============================================================*/
/* Index: RELATIONSHIP_2_FK                                     */
/*==============================================================*/
create index RELATIONSHIP_2_FK on ALGOINPUTPROD (
   PRODUCTID ASC
);

/*==============================================================*/
/* Index: RELATIONSHIP_55_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_55_FK on ALGOINPUTPROD (
   ALGORITHMID ASC
);

/*==============================================================*/
/* Table: ALGOOUTPUTPROD                                        */
/*==============================================================*/
create table ALGOOUTPUTPROD
(
   PRODUCTID            BIGINT           not null,
   ALGORITHMID          BIGINT           not null,
   constraint PK_ALGOOUTPUTPROD primary key (PRODUCTID, ALGORITHMID)
);

/*==============================================================*/
/* Index: RELATIONSHIP_21_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_21_FK on ALGOOUTPUTPROD (
   PRODUCTID ASC
);

/*==============================================================*/
/* Index: RELATIONSHIP_56_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_56_FK on ALGOOUTPUTPROD (
   ALGORITHMID ASC
);

/*==============================================================*/
/* Table: ALGOPARAMETERS                                        */
/*==============================================================*/
create table ALGOPARAMETERS
(
   ALGOPARAMETERID      BIGINT           not null,
   ALGORITHMID          BIGINT           not null,
   ALGOPARAMETERNAME    VARCHAR(255)        not null,
   ALGOPARAMETERDATATYPE VARCHAR(15)         not null,
   constraint PK_ALGOPARAMETERS primary key (ALGOPARAMETERID)
);

/*==============================================================*/
/* Index: RELATIONSHIP_58_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_58_FK on ALGOPARAMETERS (
   ALGORITHMID ASC
);

/*==============================================================*/
/* Table: ALGOPROCESSINGNODE_XREF                               */
/*==============================================================*/
create table ALGOPROCESSINGNODE_XREF
(
   RESOURCEID           BIGINT           not null,
   ALGORITHMID          BIGINT           not null,
   constraint PK_ALGOPROCESSINGNODE_XREF primary key (RESOURCEID, ALGORITHMID)
);

/*==============================================================*/
/* Index: ALGOPROCESSINGNODE_XREF_FK                            */
/*==============================================================*/
create index ALGOPROCESSINGNODE_XREF_FK on ALGOPROCESSINGNODE_XREF (
   RESOURCEID ASC
);

/*==============================================================*/
/* Index: ALGOPROCESSINGNODE_XREF2_FK                           */
/*==============================================================*/
create index ALGOPROCESSINGNODE_XREF2_FK on ALGOPROCESSINGNODE_XREF (
   ALGORITHMID ASC
);

/*==============================================================*/
/* Table: ALGORITHM                                             */
/*==============================================================*/
create table ALGORITHM
(
   ALGORITHMID          BIGINT           not null,
   ALGORITHMNAME        VARCHAR(255),
   ALGORITHMVERSION     VARCHAR(25),
   ALGORITHMNOTIFYOPSECONDS INTEGER,
   ALGORITHMTYPE        VARCHAR(12),
   ALGORITHMPCF_FILENAME VARCHAR(255),
   ALGORITHMPSF_FILENAME VARCHAR(255),
   ALGORITHMLOGFILENAME VARCHAR(255),
   ALGORITHMCOMMANDPREFIX VARCHAR(255),
   ALGORITHMEXECUTABLELOCATION VARCHAR(255),
   ALGORITHMLOGMESSAGECONTEXT VARCHAR(255),
   ALGORITHMLOGMESSAGEWARN VARCHAR(255),
   ALGORITHMLOGMESSAGEERROR VARCHAR(255),
   ALGORITHMEXECUTABLENAME VARCHAR(255)        not null,
   constraint PK_ALGORITHM primary key (ALGORITHMID)
);

/*==============================================================*/
/* Table: ARCHIVENOTIFYCONFIG                                   */
/*==============================================================*/
create table ARCHIVENOTIFYCONFIG
(
   ARCHIVENOTIFICATIONID BIGINT            not null,
   ARCHIVENAME          VARCHAR(255),
   ARCHIVEMANIFESTFILECOUNT INTEGER,
   ARCHIVECDDRPERIOD    VARCHAR(255),
   ARCHIVECDDRFREQUENCYHOURS INTEGER,
   ARCHIVEREPORTDIRECTORY VARCHAR(255),
   constraint PK_ARCHIVENOTIFYCONFIG primary key (ARCHIVENOTIFICATIONID)
);

/*==============================================================*/
/* Table: CONFIGURATIONREGISTRY                                 */
/*==============================================================*/
create table CONFIGURATIONREGISTRY
(
   CFG_ID               BIGINT           not null,
   CFGPARAMETERNAME     VARCHAR(255)        not null,
   CFGPARAMETERVALUE    VARCHAR(255)        not null,
   CFGPARAMETERDESCRIPTION VARCHAR(255)        not null,
   CFGCLASS             VARCHAR(255),
   CFGSUBCLASS          VARCHAR(255),
   CFGLASTUPDATE        TIMESTAMP            not null,
   constraint PK_CONFIGURATIONREGISTRY primary key (CFG_ID)
);

/*  NEW, 3/25/2016 PGM */

create index CR_CPN_IDX on CONFIGURATIONREGISTRY(CFGPARAMETERNAME);

/*==============================================================*/
/* Table: CUSTOMERPOINTSOFCONTACT                               */
/*==============================================================*/
create table CUSTOMERPOINTSOFCONTACT
(
   POCID                INTEGER            not null,
   CUSTPROFILEID        BIGINT           not null,
   CUSTROLE             VARCHAR(255)        not null,
   CUSTPRIMARYPOC       SMALLINT             not null,
   constraint PK_CUSTOMERPOINTSOFCONTACT primary key (POCID, CUSTPROFILEID, CUSTROLE)
);

/*==============================================================*/
/* Index: RELATIONSHIP_28_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_28_FK on CUSTOMERPOINTSOFCONTACT (
   POCID ASC
);

/*==============================================================*/
/* Index: RELATIONSHIP_29_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_29_FK on CUSTOMERPOINTSOFCONTACT (
   CUSTPROFILEID ASC
);

/*==============================================================*/
/* Table: CUSTOMERPROFILE                                       */
/*==============================================================*/
create table CUSTOMERPROFILE
(
   CUSTPROFILEID        BIGINT           not null,
   CUSTIDENTIFIER       VARCHAR(255)        not null,
   CUSTPROFILETRUSTEDUSER SMALLINT             not null,
   CUSTPORTALAUTHENTICATION VARCHAR(255),
   CUSTPROFILEAPPROVALTHRESHOLD VARCHAR(255),
   CUSTPULLUSERID       VARCHAR(255),
   CUSTPROFILEACTIVEFLAG SMALLINT             not null,
   CUSTPORTAL_PW_LASTUPDATETIME TIMESTAMP            not null,
   constraint PK_CUSTOMERPROFILE primary key (CUSTPROFILEID)
);

/*==============================================================*/
/* Table: DATADENIALFLAG                                        */
/*==============================================================*/
create table DATADENIALFLAG
(
   DDFLAG               INTEGER              not null,
   constraint PK_DATADENIALFLAG primary key (DDFLAG)
);

/*==============================================================*/
/* Table: DELIVERYNOTIFICATIONTYPE                              */
/*==============================================================*/
create table DELIVERYNOTIFICATIONTYPE
(
   DELIVERYNOTIFICATIONTYPEID BIGINT           not null,
   DELIVERYNOTIFICATIONNAME VARCHAR(255),
   constraint PK_DELIVERYNOTIFICATIONTYPE primary key (DELIVERYNOTIFICATIONTYPEID)
);

/*==============================================================*/
/* Table: DISJOBCLASSCODE                                       */
/*==============================================================*/
create table DISJOBCLASSCODE
(
   DICLASS              INTEGER            not null,
   DICLASSDESCRIPTION   VARCHAR(255),
   constraint PK_DISJOBCLASSCODE primary key (DICLASS)
);

/*==============================================================*/
/* Table: DISJOBPRIORITYCODE                                    */
/*==============================================================*/
create table DISJOBPRIORITYCODE
(
   DIPRIORITY           INTEGER            not null,
   DIPRIORITYDESCRIPTION VARCHAR(255)        not null,
   constraint PK_DISJOBPRIORITYCODE primary key (DIPRIORITY)
);

/*==============================================================*/
/* Table: DISPULLJOBCOMPLETIONTIME                              */
/*==============================================================*/
create table DISPULLJOBCOMPLETIONTIME
(
   DIJOBID              BIGINT           not null,
   PULLCOMPLETIONTIME   TIMESTAMP            not null,
   constraint PK_DISPULLJOBCOMPLETIONTIME primary key (DIJOBID, PULLCOMPLETIONTIME)
);

/*==============================================================*/
/* Index: RELATIONSHIP_130_FK                                   */
/*==============================================================*/
create index RELATIONSHIP_130_FK on DISPULLJOBCOMPLETIONTIME (
   DIJOBID ASC
);

/*==============================================================*/
/* Table: DISTPREPAREREQUESTLOCK                                */
/*==============================================================*/
create table DISTPREPAREREQUESTLOCK
(
   LOCKCOLUMN           INTEGER
);

/*==============================================================*/
/* Table: DISTRIBUTIONJOB                                       */
/*==============================================================*/
create table DISTRIBUTIONJOB
(
   DIJOBID              BIGINT           not null,
   DIREQID              BIGINT,
   RESOURCEID           BIGINT,
   DIJOBSTATUS          VARCHAR(15)         not null,
   DIJOBENQUEUETIME     TIMESTAMP,
   DIJOBSTARTTIME       TIMESTAMP,
   DIJOBCOMPLETIONTIME  TIMESTAMP,
   DICHECKSUMVALUE      VARCHAR(255),
   DIFILENAME           VARCHAR(255),
   DIFILESIZE           INTEGER,
   DIJOBFAILUREREASON   VARCHAR(255),
   DIJOBRETRYCOUNT      INTEGER,
   constraint PK_DISTRIBUTIONJOB primary key (DIJOBID)
);

/*==============================================================*/
/* Index: DIREQ_DIJOB_REF_FK                                    */
/*==============================================================*/
create index DIREQ_DIJOB_REF_FK on DISTRIBUTIONJOB (
   DIREQID ASC
);

/*==============================================================*/
/* Index: DINODE_DIJOB_REF_FK                                   */
/*==============================================================*/
create index DINODE_DIJOB_REF_FK on DISTRIBUTIONJOB (
   RESOURCEID ASC
);

/*==============================================================*/
/* Index: DJ_STATUS_ENQTIME                                     */
/*==============================================================*/
create index DJ_STATUS_ENQTIME on DISTRIBUTIONJOB (
   DIJOBSTATUS ASC,
   DIJOBENQUEUETIME ASC
);

/*==============================================================*/
/* Index: DJ_FILENAME                                           */
/*==============================================================*/
create index DJ_FILENAME on DISTRIBUTIONJOB (
   DIFILENAME ASC
);

/*==============================================================*/
/* Table: DISTRIBUTIONJOBEXTERNALHOST                           */
/*==============================================================*/
create table DISTRIBUTIONJOBEXTERNALHOST
(
   DIJOBID              BIGINT           not null,
   DIASSIGNEDHOSTID     BIGINT           not null,
   RESOURCEID           BIGINT,
   constraint PK_DISTRIBUTIONJOBEXTERNALHOST primary key (DIJOBID, DIASSIGNEDHOSTID)
);

/*==============================================================*/
/* Index: DIJEH_DN_REF_FK                                       */
/*==============================================================*/
create index DIJEH_DN_REF_FK on DISTRIBUTIONJOBEXTERNALHOST (
   RESOURCEID ASC
);

/*==============================================================*/
/* Index: DJ_DJEH_REF_FK                                        */
/*==============================================================*/
create index DJ_DJEH_REF_FK on DISTRIBUTIONJOBEXTERNALHOST (
   DIJOBID ASC
);

/*==============================================================*/
/* Table: DISTRIBUTIONJOBLOCK                                   */
/*==============================================================*/
create table DISTRIBUTIONJOBLOCK
(
   LOCKCOLUMN           INTEGER
);

/*==============================================================*/
/* Table: DISTRIBUTIONNODE                                      */
/*==============================================================*/
create table DISTRIBUTIONNODE
(
   RESOURCEID           BIGINT           not null,
   constraint PK_DISTRIBUTIONNODE primary key (RESOURCEID)
);

/*==============================================================*/
/* Table: DISTRIBUTIONNODEJOBBOX                                */
/*==============================================================*/
create table DISTRIBUTIONNODEJOBBOX
(
   DNJBID               INTEGER            not null,
   DICLASS              INTEGER            not null,
   DIPRIORITY           INTEGER            not null,
   RESOURCEID           BIGINT           not null,
   DNJBNUMBOXES         INTEGER              not null,
   constraint PK_DISTRIBUTIONNODEJOBBOX primary key (DNJBID)
);

/*==============================================================*/
/* Index: DISTRIBUTIONNODEDNJB_REF_FK                           */
/*==============================================================*/
create index DISTRIBUTIONNODEDNJB_REF_FK on DISTRIBUTIONNODEJOBBOX (
   RESOURCEID ASC
);

/*==============================================================*/
/* Index: DIJBDIJOBPR_REF_FK                                    */
/*==============================================================*/
create index DIJBDIJOBPR_REF_FK on DISTRIBUTIONNODEJOBBOX (
   DIPRIORITY ASC
);

/*==============================================================*/
/* Index: DIJBDIJOBCLASS_REF_FK                                 */
/*==============================================================*/
create index DIJBDIJOBCLASS_REF_FK on DISTRIBUTIONNODEJOBBOX (
   DICLASS ASC
);

/*==============================================================*/
/* Table: DISTRIBUTIONNOTIFYRETRIEVEJOB                         */
/*==============================================================*/
create table DISTRIBUTIONNOTIFYRETRIEVEJOB
(
   LOCKCOLUMN           INTEGER
);

/*==============================================================*/
/* Table: DISTRIBUTIONPREPAREREQUEST                            */
/*==============================================================*/
create table DISTRIBUTIONPREPAREREQUEST
(
   DPRREQID             BIGINT           not null,
   FILEID               BIGINT           not null,
   DPRCOMPRESSIONTYPE   VARCHAR(25),
   DPRCHECKSUMTYPE      VARCHAR(12),
   DPRREQSTATUS         VARCHAR(15),
   DPRREQENQUEUETIME    TIMESTAMP,
   DPRREQCOMPLETIONTIME TIMESTAMP,
   DPRCREATERESOURCEID  BIGINT,
   DPRCOMPLETERESOURCEID BIGINT,
   constraint PK_DISTRIBUTIONPREPAREREQUEST primary key (DPRREQID)
);

/*==============================================================*/
/* Index: DPR_FILEMETADATA_REF_FK                               */
/*==============================================================*/
create index DPR_FILEMETADATA_REF_FK on DISTRIBUTIONPREPAREREQUEST (
   FILEID ASC
);

/*==============================================================*/
/* Index: DPR_STATUS_NQTIME                                     */
/*==============================================================*/
create index DPR_STATUS_NQTIME on DISTRIBUTIONPREPAREREQUEST (
   DPRREQSTATUS ASC,
   DPRREQENQUEUETIME ASC
);

/*==============================================================*/
/* Table: DISTRIBUTIONREQUEST                                   */
/*==============================================================*/
create table DISTRIBUTIONREQUEST
(
   DIREQID              BIGINT           not null,
   DPRREQID             BIGINT           not null,
   DIPRIORITY           INTEGER            not null,
   SUBSCRIPTIONID       INTEGER            not null,
   DICLASS              INTEGER            not null,
   constraint PK_DISTRIBUTIONREQUEST primary key (DIREQID)
);

/*==============================================================*/
/* Index: SUB_DIREQ_REF_FK                                      */
/*==============================================================*/
create index SUB_DIREQ_REF_FK on DISTRIBUTIONREQUEST (
   SUBSCRIPTIONID ASC
);

/*==============================================================*/
/* Index: DR_DPR_XREF_FK                                        */
/*==============================================================*/
create index DR_DPR_XREF_FK on DISTRIBUTIONREQUEST (
   DPRREQID ASC
);

/*==============================================================*/
/* Index: DIREQDIJOBPRCODE_REF_FK                               */
/*==============================================================*/
create index DIREQDIJOBPRCODE_REF_FK on DISTRIBUTIONREQUEST (
   DIPRIORITY ASC
);

/*==============================================================*/
/* Index: DIREQDIJOBCLASSCODE_REF_FK                            */
/*==============================================================*/
create index DIREQDIJOBCLASSCODE_REF_FK on DISTRIBUTIONREQUEST (
   DICLASS ASC
);

/*==============================================================*/
/* Table: DISTRIBUTIONRETREIVEJOBLOCK                           */
/*==============================================================*/
create table DISTRIBUTIONRETREIVEJOBLOCK
(
   LOCKCOLUMN           INTEGER
);

/*==============================================================*/
/* Table: ENTERPRISEDIMENSION                                   */
/*==============================================================*/
create table ENTERPRISEDIMENSION
(
   E_DIMENSIONID        INTEGER            not null,
   E_DIMENSIONNAME      VARCHAR(255),
   E_DIMENSIONSTART     VARCHAR(1024),
   E_DIMENSIONINTERVAL  VARCHAR(1024),
   E_DIMENSIONEND       VARCHAR(1024),
   E_DIMENSIONSTORAGESIZE INTEGER,
   E_DIMENSIONSTORAGEMAXSIZE INTEGER,
   constraint PK_ENTERPRISEDIMENSION primary key (E_DIMENSIONID)
);

/*==============================================================*/
/* Table: ENTERPRISEDIMENSIONLIST                               */
/*==============================================================*/
create table ENTERPRISEDIMENSIONLIST
(
   E_DIMENSIONLISTID    INTEGER            not null,
   E_DIMENSIONLISTNAME  VARCHAR(255),
   constraint PK_ENTERPRISEDIMENSIONLIST primary key (E_DIMENSIONLISTID)
);

/*==============================================================*/
/* Table: ENTERPRISEMEASURE                                     */
/*==============================================================*/
create table ENTERPRISEMEASURE
(
   MEASUREID            INTEGER            not null,
   E_DIMENSIONLISTID    INTEGER            not null,
   MEASURENAME          VARCHAR(255),
   MEASUREDATATYPE      VARCHAR(255),
   constraint PK_ENTERPRISEMEASURE primary key (MEASUREID)
);

/*==============================================================*/
/* Index: FK_EM_EDL_FK                                          */
/*==============================================================*/
create index FK_EM_EDL_FK on ENTERPRISEMEASURE (
   E_DIMENSIONLISTID ASC
);

/*==============================================================*/
/* Table: ENTERPRISEORDEREDDIMENSION                            */
/*==============================================================*/
create table ENTERPRISEORDEREDDIMENSION
(
   E_DIMENSIONID        INTEGER            not null,
   E_DIMENSIONLISTID    INTEGER            not null,
   E_DIMENSIONORDER     SMALLINT             not null,
   E_DIMENSIONDATAPARTITION SMALLINT,
   constraint PK_ENTERPRISEORDEREDDIMENSION primary key (E_DIMENSIONID, E_DIMENSIONLISTID, E_DIMENSIONORDER)
);

/*==============================================================*/
/* Index: FK_EOD_ENTERPRISEDIM_FK                               */
/*==============================================================*/
create index FK_EOD_ENTERPRISEDIM_FK on ENTERPRISEORDEREDDIMENSION (
   E_DIMENSIONID ASC
);

/*==============================================================*/
/* Index: FK_EOD_EDL_FK                                         */
/*==============================================================*/
create index FK_EOD_EDL_FK on ENTERPRISEORDEREDDIMENSION (
   E_DIMENSIONLISTID ASC
);

/*==============================================================*/
/* Table: EXTERNALDATAHOST                                      */
/*==============================================================*/
create table EXTERNALDATAHOST
(
   HOSTID               BIGINT           not null,
   HOSTNAME             VARCHAR(255),
   HOSTADDRESS          VARCHAR(255),
   HOSTSTATUS           VARCHAR(4),
   HOSTACCESSTYPE       VARCHAR(12),
   HOSTACCESSDESCRIPTION VARCHAR(255),
   HOSTACCESSLOGINID    VARCHAR(255),
   HOSTACCESSPASSWORD   VARCHAR(255),
   MAXCONCURRENTCONNECTIONS BIGINT            not null,
   CERTIFICATES         VARCHAR(255),
   HOSTNUMRETRIES       INTEGER,
   HOSTRETRYWAITSECONDS INTEGER,
   constraint PK_EXTERNALDATAHOST primary key (HOSTID)
);

/*==============================================================*/
/* Table: FILEMETADATA                                          */
/*==============================================================*/
create table FILEMETADATA
(
   FILEID               BIGINT           not null,
   PRODUCTID            BIGINT           not null,
   PGDATAPARTITIONID    INTEGER,
   FILEINSERTTIME       TIMESTAMP,
   FILESIZE             INTEGER,
   FILESTARTTIME        TIMESTAMP,
   FILEENDTIME          TIMESTAMP,
   FILEBEGINORBITNUM    INTEGER,
   FILEENDORBITNUM      INTEGER,
   FILEIDPSGRANULEVERSION VARCHAR(255),
   FILEDAYNIGHTFLAG     VARCHAR(5),
   FILESPATIALAREA      geography(MULTIPOLYGON, 4326),
   FILENAME             VARCHAR(255),
   FILEMETADATAXML      XML,
   FILEVALIDATIONRESULTS XML,
   FILEASCDESCINDICATOR SMALLINT,
   FILEDELETEDFLAG      CHAR(1),
   constraint PK_FILEMETADATA primary key (FILEID)
);

/*==============================================================*/
/* Index: FM_PRODENDTIME                                        */
/*==============================================================*/
create index FM_PRODENDTIME on FILEMETADATA (
   PRODUCTID ASC,
   FILEENDTIME ASC
);

/*==============================================================*/
/* Index: RELATIONSHIP_9_FK                                     */
/*==============================================================*/
create index RELATIONSHIP_9_FK on FILEMETADATA (
   PRODUCTID ASC
);

/*==============================================================*/
/* Index: RELATIONSHIP_106_FK                                   */
/*==============================================================*/
create index RELATIONSHIP_106_FK on FILEMETADATA (
   PGDATAPARTITIONID ASC
);

/*==============================================================*/
/* Index: FM_INSERTTIME                                         */
/*==============================================================*/
create index FM_INSERTTIME on FILEMETADATA (
   FILEINSERTTIME ASC
);

/*==============================================================*/
/* Table: FILEQUALITYSUMMARY                                    */
/*==============================================================*/
create table FILEQUALITYSUMMARY
(
   PRODUCTID            BIGINT           not null,
   PRODUCTQUALITYSUMMARYNAME VARCHAR(255)        not null,
   FILEID               BIGINT           not null,
   FILEQUALITYSUMMARYVALUE VARCHAR(255),
   constraint PK_FILEQUALITYSUMMARY primary key (PRODUCTID, PRODUCTQUALITYSUMMARYNAME, FILEID)
);

/*==============================================================*/
/* Index: RELATIONSHIP_7_FK                                     */
/*==============================================================*/
create index RELATIONSHIP_7_FK on FILEQUALITYSUMMARY (
   PRODUCTID ASC,
   PRODUCTQUALITYSUMMARYNAME ASC
);

/*==============================================================*/
/* Index: RELATIONSHIP_8_FK                                     */
/*==============================================================*/
create index RELATIONSHIP_8_FK on FILEQUALITYSUMMARY (
   FILEID ASC
);

/*==============================================================*/
/* Table: FILERETRANSMITCOUNT                                   */
/*==============================================================*/
create table FILERETRANSMITCOUNT
(
   BADFILENAME          VARCHAR(255)        not null,
   PRODUCTID            BIGINT           not null,
   BADFILESIZE          FLOAT,
   BADCHECKSUM          INTEGER,
   BADFILERETRANSMITCOUNT INTEGER,
   constraint PK_FILERETRANSMITCOUNT primary key (BADFILENAME)
);

/*==============================================================*/
/* Index: RELATIONSHIP_5_FK                                     */
/*==============================================================*/
create index RELATIONSHIP_5_FK on FILERETRANSMITCOUNT (
   PRODUCTID ASC
);

/*==============================================================*/
/* Table: FILERETRYHISTORY                                      */
/*==============================================================*/
create table FILERETRYHISTORY
(
   BADFILENAME          VARCHAR(255)        not null,
   FILECHECKTYPE        VARCHAR(10),
   FILECHECKRESULT      VARCHAR(255),
   FILERETRYTIME        TIMESTAMP,
   FILECHECKTIME        TIMESTAMP,
   constraint PK_FILERETRYHISTORY primary key (BADFILENAME)
);

/*==============================================================*/
/* Table: GAZETTEER                                             */
/*==============================================================*/
create table GAZETTEER
(
   GZID                 BIGINT           not null,
   GZDESIGNATION        VARCHAR(255),
   GZFEATURENAME        VARCHAR(255),
   GZLOCATIONSPATIAL    geography(POLYGON, 4326),
   GZLOCATIONELEVATIONMETERS FLOAT,
   GZSOURCETYPE         CHAR(1),
   constraint PK_GAZETTEER primary key (GZID)
);

/*==============================================================*/
/* Table: HDF5_ARRAY                                            */
/*==============================================================*/
create table HDF5_ARRAY
(
   H_ARRAYID            INTEGER            not null,
   H_GROUPID            INTEGER            not null,
   H_ARRAYNAME          VARCHAR(255),
   H_DATATYPE           VARCHAR(255),
   constraint PK_HDF5_ARRAY primary key (H_ARRAYID)
);

/*==============================================================*/
/* Index: RELATIONSHIP_33_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_33_FK on HDF5_ARRAY (
   H_GROUPID ASC
);

/*==============================================================*/
/* Table: HDF5_ARRAYATTRIBUTE                                   */
/*==============================================================*/
create table HDF5_ARRAYATTRIBUTE
(
   H_ARRAYATTRIBUTEID   INTEGER            not null,
   H_ARRAYID            INTEGER            not null,
   H_ARRAYATTRIBUTENAME VARCHAR(255),
   H_ARRAYATTRIBUTEEXTERNALFLAG SMALLINT,
   H_ARRAYATTRIBUTEDATATYPE VARCHAR(255),
   H_ARRAYNUMATTRVALUES INTEGER,
   H_ARRAYATTRIBUTESTRINGVALUE VARCHAR(2048),
   H_ARRAYATTRIBUTEDELIMITER VARCHAR(2),
   constraint PK_HDF5_ARRAYATTRIBUTE primary key (H_ARRAYATTRIBUTEID)
);

/*==============================================================*/
/* Index: FK_H5AA_H5A_FK                                        */
/*==============================================================*/
create index FK_H5AA_H5A_FK on HDF5_ARRAYATTRIBUTE (
   H_ARRAYID ASC
);

/*==============================================================*/
/* Table: HDF5_DIMENSIONLIST                                    */
/*==============================================================*/
create table HDF5_DIMENSIONLIST
(
   H_ARRAYID            INTEGER            not null,
   H_DIMENSIONORDER     SMALLINT             not null,
   H_DIMENSIONSIZE      INTEGER,
   H_DIMENSIONMAXIMUMSIZE VARCHAR(32),
   H_UNLIMITEDFLAG      SMALLINT,
   H_DATAPARTITION      SMALLINT,
   constraint PK_HDF5_DIMENSIONLIST primary key (H_ARRAYID, H_DIMENSIONORDER)
);

/*==============================================================*/
/* Index: RELATIONSHIP_3_FK                                     */
/*==============================================================*/
create index RELATIONSHIP_3_FK on HDF5_DIMENSIONLIST (
   H_ARRAYID ASC
);

/*==============================================================*/
/* Table: HDF5_GROUP                                            */
/*==============================================================*/
create table HDF5_GROUP
(
   H_GROUPID            INTEGER            not null,
   HDF_H_GROUPID        INTEGER,
   PRODUCTID            BIGINT           not null,
   H_GROUPNAME          VARCHAR(1024),
   constraint PK_HDF5_GROUP primary key (H_GROUPID)
);

/*==============================================================*/
/* Index: RELATIONSHIP_25_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_25_FK on HDF5_GROUP (
   PRODUCTID ASC
);

/*==============================================================*/
/* Index: FK_HDF5_GROUP_SELF_FK                                 */
/*==============================================================*/
create index FK_HDF5_GROUP_SELF_FK on HDF5_GROUP (
   HDF_H_GROUPID ASC
);

/*==============================================================*/
/* Table: HDF5_GROUPATTRIBUTE                                   */
/*==============================================================*/
create table HDF5_GROUPATTRIBUTE
(
   H_GROUPATTRIBUTEID   INTEGER            not null,
   H_GROUPID            INTEGER            not null,
   H_GROUPATTRIBUTENAME VARCHAR(255),
   H_GROUPATTRIBUTEDATATYPE VARCHAR(255),
   H_GROUPATTRIBUTEEXTERNALFLAG SMALLINT,
   H_GROUPNUMATTRVALUES INTEGER,
   H_GROUPATTRIBUTESTRINGVALUE VARCHAR(2048),
   H_GROUPATTRIBUTEDELIMITER VARCHAR(2),
   constraint PK_HDF5_GROUPATTRIBUTE primary key (H_GROUPATTRIBUTEID)
);

/*==============================================================*/
/* Index: RELATIONSHIP_51_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_51_FK on HDF5_GROUPATTRIBUTE (
   H_GROUPID ASC
);

/*==============================================================*/
/* Table: HDF5_STRUCTURE                                        */
/*==============================================================*/
create table HDF5_STRUCTURE
(
   PRODUCTID            BIGINT           not null,
   constraint PK_HDF5_STRUCTURE primary key (PRODUCTID)
);

/*==============================================================*/
/* Table: INGESTINCOMINGDIRECTORY                               */
/*==============================================================*/
create table INGESTINCOMINGDIRECTORY
(
   INGESTINCOMINGDIRECTORYID INTEGER            not null,
   INGESTDIRECTORYNAME  VARCHAR(255),
   constraint PK_INGESTINCOMINGDIRECTORY primary key (INGESTINCOMINGDIRECTORYID)
);

/*==============================================================*/
/* Table: INGESTPROCESSSTEP                                     */
/*==============================================================*/
create table INGESTPROCESSSTEP
(
   PRODUCTID            BIGINT           not null,
   NSFID                INTEGER            not null,
   IPSOPTIONALPARAMETERS VARCHAR(255),
   IPSFAILSINGEST       SMALLINT,
   IPSENABLE            SMALLINT,
   IPSDORETRANSMIT      SMALLINT,
   constraint PK_INGESTPROCESSSTEP primary key (PRODUCTID, NSFID)
);

/*==============================================================*/
/* Index: RELATIONSHIP_66_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_66_FK on INGESTPROCESSSTEP (
   PRODUCTID ASC
);

/*==============================================================*/
/* Index: RELATIONSHIP_77_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_77_FK on INGESTPROCESSSTEP (
   NSFID ASC
);

/*==============================================================*/
/* Table: INGESTREQUESTBUFFER                                   */
/*==============================================================*/
create table INGESTREQUESTBUFFER
(
   IR_ID                BIGINT           not null,
   IRBSTATUS            VARCHAR(32),
   IRBINSERTTIME        VARCHAR(50),
   IRBMESSAGE           VARCHAR(1024),
   constraint PK_INGESTREQUESTBUFFER primary key (IR_ID)
);

/*==============================================================*/
/* Table: INGESTREQUESTLOG                                      */
/*==============================================================*/
create table INGESTREQUESTLOG
(
   IR_ID                BIGINT           not null,
   PRODUCTID            BIGINT,
   INGESTINCOMINGDIRECTORYID INTEGER            not null,
   IRMESSAGECREATETIME  TIMESTAMP            not null,
   IRSTARTTIME          TIMESTAMP,
   IRCOMPLETIONTIME     TIMESTAMP,
   IRFILENAME           VARCHAR(255)        not null,
   IRSTATUS             VARCHAR(32)         not null,
   IRFAILUREREASON      VARCHAR(32),
   IRDETECTRESOURCEID   BIGINT,
   IRCATALOGRESOURCEID  BIGINT,
   IRENQUEUETIME        TIMESTAMP,
   IRFILECREATETIME     TIMESTAMP,
   IROBSSTARTTIME       TIMESTAMP,
   IRORIGINALCRCVALUE   VARCHAR(255),
   constraint PK_INGESTREQUESTLOG primary key (IR_ID)
);

/*==============================================================*/
/* Table: INGESTTHROTTLELOCK                                    */
/*==============================================================*/
create table INGESTTHROTTLELOCK
(
   LOCKCOLUMN           INTEGER
);

/*==============================================================*/
/* Index: RELATIONSHIP_126_FK                                   */
/*==============================================================*/
create index RELATIONSHIP_126_FK on INGESTREQUESTLOG (
   INGESTINCOMINGDIRECTORYID ASC
);

/*==============================================================*/
/* Index: RELATIONSHIP_124_FK                                   */
/*==============================================================*/
create index RELATIONSHIP_124_FK on INGESTREQUESTLOG (
   PRODUCTID ASC
);

/*==============================================================*/
/* Index: IRL_COMPLETIONTIME                                    */
/*==============================================================*/
create index IRL_COMPLETIONTIME on INGESTREQUESTLOG (
   IRCOMPLETIONTIME ASC
);

/*==============================================================*/
/* Index: IRL_STATUS_OBSTIME                                    */
/*==============================================================*/
create index IRL_STATUS_OBSTIME on INGESTREQUESTLOG (
   IRSTATUS ASC,
   IROBSSTARTTIME DESC
);

/*==============================================================*/
/* Index: IRL_ENQ_DETECT_IX                                  */
/*==============================================================*/
create index IRL_ENQ_DETECT_IX on INGESTREQUESTLOG (
   IRENQUEUETIME ASC,
   IRDETECTRESOURCEID ASC
);

/*==============================================================*/
/* Table: JOBCLASSCODE                                          */
/*==============================================================*/
create table JOBCLASSCODE
(
   JOBCLASS             INTEGER              not null,
   JOBCLASSDESCRIPTION  VARCHAR(255)        not null,
   constraint PK_JOBCLASSCODE primary key (JOBCLASS)
);

/*==============================================================*/
/* Table: JOBPRIORITYCODE                                       */
/*==============================================================*/
create table JOBPRIORITYCODE
(
   JOBPRIORITY          INTEGER              not null,
   JOBPRIORITYDESCRIPTION VARCHAR(255)        not null,
   constraint PK_JOBPRIORITYCODE primary key (JOBPRIORITY)
);

/*==============================================================*/
/* Table: JOBSPECINPUT                                          */
/*==============================================================*/
create table JOBSPECINPUT
(
   JSIID                BIGINT           not null,
   FILEID               BIGINT,
   PRODPARTIALJOBID     BIGINT           not null,
   PRIPID               BIGINT,
   PRINPUTPREFERENCE    INTEGER,
   JSIOBSSTARTTIME      TIMESTAMP,
   JSIOBSENDTIME        TIMESTAMP,
   JSITIMEOUTTIME       TIMESTAMP,
   JSIFILEHANDLE        VARCHAR(255),
   JSILUN_SEQNUM        INTEGER,
   JSIPREDICTEDFILENAME VARCHAR(255),
   constraint PK_JOBSPECINPUT primary key (JSIID)
);

/*==============================================================*/
/* Index: RELATIONSHIP_45_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_45_FK on JOBSPECINPUT (
   PRODPARTIALJOBID ASC
);

/*==============================================================*/
/* Index: RELATIONSHIP_75_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_75_FK on JOBSPECINPUT (
   FILEID ASC
);

/*==============================================================*/
/* Index: RELATIONSHIP_74_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_74_FK on JOBSPECINPUT (
   PRIPID ASC,
   PRINPUTPREFERENCE ASC
);

/*==============================================================*/
/* Table: JOBSPECOUTPUT                                         */
/*==============================================================*/
create table JOBSPECOUTPUT
(
   JSOID                BIGINT           not null,
   PROSID               BIGINT           not null,
   PRODPARTIALJOBID     BIGINT           not null,
   JSOPREDICTEDFILENAME VARCHAR(255),
   JSOPREDICTEDOBSSTARTTIME TIMESTAMP,
   JSOPREDICTEDOBSENDTIME TIMESTAMP,
   constraint PK_JOBSPECOUTPUT primary key (JSOID)
);

/*==============================================================*/
/* Index: RELATIONSHIP_44_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_44_FK on JOBSPECOUTPUT (
   PRODPARTIALJOBID ASC
);

/*==============================================================*/
/* Index: RELATIONSHIP_48_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_48_FK on JOBSPECOUTPUT (
   PROSID ASC
);

/*==============================================================*/
/* Table: JOBSPECPARAMETERS                                     */
/*==============================================================*/
create table JOBSPECPARAMETERS
(
   JSPID                BIGINT           not null,
   PRODPARTIALJOBID     BIGINT           not null,
   PRPARAMETERSEQNO     BIGINT           not null,
   JSPARAMETERVALUE     VARCHAR(255),
   constraint PK_JOBSPECPARAMETERS primary key (JSPID)
);

/*==============================================================*/
/* Index: RELATIONSHIP_43_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_43_FK on JOBSPECPARAMETERS (
   PRODPARTIALJOBID ASC
);

/*==============================================================*/
/* Index: RELATIONSHIP_46_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_46_FK on JOBSPECPARAMETERS (
   PRPARAMETERSEQNO ASC
);

/*==============================================================*/
/* Table: MEASURE_H_ARRAY_XREF                                  */
/*==============================================================*/
create table MEASURE_H_ARRAY_XREF
(
   MEASUREID            INTEGER            not null,
   H_ARRAYID            INTEGER            not null,
   H_TRANSFORMTYPE      VARCHAR(255),
   H_BITOFFSET          SMALLINT,
   H_BITLENGTH          SMALLINT,
   H_SCALEFACTORREFERENCE VARCHAR(255),
   H_ADDOFFSETREFERENCE VARCHAR(255),
   constraint PK_MEASURE_H_ARRAY_XREF primary key (MEASUREID, H_ARRAYID)
);

/*==============================================================*/
/* Index: FK_MHAX_M_FK                                          */
/*==============================================================*/
create index FK_MHAX_M_FK on MEASURE_H_ARRAY_XREF (
   MEASUREID ASC
);

/*==============================================================*/
/* Index: FK_H_ARRAY_XREF_MHA_FK                                */
/*==============================================================*/
create index FK_H_ARRAY_XREF_MHA_FK on MEASURE_H_ARRAY_XREF (
   H_ARRAYID ASC
);

/*==============================================================*/
/* Table: MEASURE_N_ARRAY_XREF                                  */
/*==============================================================*/
create table MEASURE_N_ARRAY_XREF
(
   N_ARRAYID            INTEGER            not null,
   MEASUREID            INTEGER            not null,
   N_TRANSFORMTYPE      VARCHAR(255),
   N_BITOFFSET          SMALLINT,
   N_BITLENGTH          SMALLINT,
   N_SCALEFACTORREFERENCE VARCHAR(255),
   N_ADDOFFSETREFERENCE VARCHAR(255),
   constraint PK_MEASURE_N_ARRAY_XREF primary key (N_ARRAYID, MEASUREID)
);

/*==============================================================*/
/* Index: RELATIONSHIP_38_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_38_FK on MEASURE_N_ARRAY_XREF (
   N_ARRAYID ASC
);

/*==============================================================*/
/* Index: FK_MNAX_M_FK                                          */
/*==============================================================*/
create index FK_MNAX_M_FK on MEASURE_N_ARRAY_XREF (
   MEASUREID ASC
);

/*==============================================================*/
/* Table: METEOOBSVARIABLES                                     */
/*==============================================================*/
create table METEOOBSVARIABLES
(
   MOVCODE              VARCHAR(8)          not null,
   MOVVARIABLENAME      VARCHAR(255),
   MOVUNITS             VARCHAR(255),
   constraint PK_METEOOBSVARIABLES primary key (MOVCODE)
);

/*==============================================================*/
/* Table: METEOPOINTOBS                                         */
/*==============================================================*/
create table METEOPOINTOBS
(
   MPOID                BIGINT           not null,
   MOVCODE              VARCHAR(8)          not null,
   MSBLOCKNUMBER        SMALLINT,
   MSSTATIONNUMBER      SMALLINT,
   MPOLOCATIONPOINT     geography(POLYGON, 4326),
   MPOELEVATIONMETERS   FLOAT,
   MPOOBSERVATIONTIME   TIMESTAMP,
   MPOOBSERVATIONVALUE  VARCHAR(150),
   constraint PK_METEOPOINTOBS primary key (MPOID)
);

/*==============================================================*/
/* Index: RELATIONSHIP_61_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_61_FK on METEOPOINTOBS (
   MSBLOCKNUMBER ASC,
   MSSTATIONNUMBER ASC
);

/*==============================================================*/
/* Index: RELATIONSHIP_62_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_62_FK on METEOPOINTOBS (
   MOVCODE ASC
);

/*==============================================================*/
/* Table: METEOSTATION                                          */
/*==============================================================*/
create table METEOSTATION
(
   MSBLOCKNUMBER        SMALLINT             not null,
   MSSTATIONNUMBER      SMALLINT             not null,
   MSICAOLOCATIONINDICATOR CHAR(4),
   MSPLACENAME          VARCHAR(255),
   MSSTATE              CHAR(2),
   MSCOUNTRYNAME        VARCHAR(55),
   MSWMOREGION          CHAR(7),
   MSSTATIONLOCATION    geography(POLYGON, 4326),
   MSUPPERAIRLOCATION   geography(POLYGON, 4326),
   MSSTATIONELEVATIONMETERS FLOAT,
   MSUPPERAIRELEVATIONMETERS FLOAT,
   constraint PK_METEOSTATION primary key (MSBLOCKNUMBER, MSSTATIONNUMBER)
);

/*==============================================================*/
/* Table: MHA_SUBSET                                            */
/*==============================================================*/
create table MHA_SUBSET
(
   MEASUREID            INTEGER            not null,
   H_ARRAYID            INTEGER            not null,
   MHADIMENSIONORDER    SMALLINT             not null,
   MHASTART             INTEGER,
   MHASTRIDE            INTEGER,
   MHACOUNT             INTEGER,
   constraint PK_MHA_SUBSET primary key (MEASUREID, H_ARRAYID, MHADIMENSIONORDER)
);

/*==============================================================*/
/* Index: RELATIONSHIP_36_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_36_FK on MHA_SUBSET (
   MEASUREID ASC,
   H_ARRAYID ASC
);

/*==============================================================*/
/* Table: MNA_SUBSET                                            */
/*==============================================================*/
create table MNA_SUBSET
(
   N_ARRAYID            INTEGER            not null,
   MEASUREID            INTEGER            not null,
   MNADIMENSIONORDER    SMALLINT             not null,
   MNASTART             INTEGER,
   MNASTRIDE            INTEGER,
   MNACOUNT             INTEGER,
   constraint PK_MNA_SUBSET primary key (N_ARRAYID, MEASUREID, MNADIMENSIONORDER)
);

/*==============================================================*/
/* Index: RELATIONSHIP_71_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_71_FK on MNA_SUBSET (
   N_ARRAYID ASC,
   MEASUREID ASC
);

/*==============================================================*/
/* Table: NC4_ARRAY                                             */
/*==============================================================*/
create table NC4_ARRAY
(
   N_ARRAYID            INTEGER            not null,
   N_GROUPID            INTEGER            not null,
   N_ARRAYNAME          VARCHAR(255),
   N_DATATYPE           VARCHAR(255),
   constraint PK_NC4_ARRAY primary key (N_ARRAYID)
);

/*==============================================================*/
/* Index: FK_NA_NG_FK                                           */
/*==============================================================*/
create index FK_NA_NG_FK on NC4_ARRAY (
   N_GROUPID ASC
);

/*==============================================================*/
/* Table: NC4_ARRAYATTRIBUTE                                    */
/*==============================================================*/
create table NC4_ARRAYATTRIBUTE
(
   N_ARRAYATTRIBUTEID   INTEGER            not null,
   N_ARRAYID            INTEGER            not null,
   N_ARRAYATTRIBUTENAME VARCHAR(255),
   N_ARRAYATTRIBUTEDATATYPE VARCHAR(255),
   N_ARRAYATTRIBUTEEXTERNALFLAG SMALLINT,
   N_ARRAYATTRIBUTEDELIMITER VARCHAR(2),
   N_ARRAYATTRIBUTESTRINGVALUE VARCHAR(2048),
   N_ARRAYNUMATTRVALUES INTEGER,
   constraint PK_NC4_ARRAYATTRIBUTE primary key (N_ARRAYATTRIBUTEID)
);

/*==============================================================*/
/* Index: FK_N4AA_N4A_FK                                        */
/*==============================================================*/
create index FK_N4AA_N4A_FK on NC4_ARRAYATTRIBUTE (
   N_ARRAYID ASC
);

/*==============================================================*/
/* Table: NC4_DIMENSION                                         */
/*==============================================================*/
create table NC4_DIMENSION
(
   N_DIMENSIONID        INTEGER            not null,
   N_GROUPID            INTEGER            not null,
   N_DIMENSIONNAME      VARCHAR(255),
   N_DIMENSIONSIZE      INTEGER,
   N_DIMENSIONUNLIMITEDFLAG SMALLINT,
   N_DIMENSIONMAXIMUMSIZE INTEGER,
   constraint PK_NC4_DIMENSION primary key (N_DIMENSIONID)
);

/*==============================================================*/
/* Index: FK_N4D_N4G_FK                                         */
/*==============================================================*/
create index FK_N4D_N4G_FK on NC4_DIMENSION (
   N_GROUPID ASC
);

/*==============================================================*/
/* Table: NC4_DIMENSIONLIST                                     */
/*==============================================================*/
create table NC4_DIMENSIONLIST
(
   N_ARRAYID            INTEGER            not null,
   N_DIMENSIONORDER     SMALLINT             not null,
   N_DIMENSIONID        INTEGER            not null,
   N_DATAPARTITION      SMALLINT,
   constraint PK_NC4_DIMENSIONLIST primary key (N_ARRAYID, N_DIMENSIONORDER)
);

/*==============================================================*/
/* Index: FK_NDL_NA_FK                                          */
/*==============================================================*/
create index FK_NDL_NA_FK on NC4_DIMENSIONLIST (
   N_ARRAYID ASC
);

/*==============================================================*/
/* Index: FK_N4_DIML_N4_DIM_FK                                  */
/*==============================================================*/
create index FK_N4_DIML_N4_DIM_FK on NC4_DIMENSIONLIST (
   N_DIMENSIONID ASC
);

/*==============================================================*/
/* Table: NC4_GROUP                                             */
/*==============================================================*/
create table NC4_GROUP
(
   N_GROUPID            INTEGER            not null,
   NC4_N_GROUPID        INTEGER,
   PRODUCTID            BIGINT           not null,
   N_GROUPNAME          VARCHAR(255),
   constraint PK_NC4_GROUP primary key (N_GROUPID)
);

/*==============================================================*/
/* Index: FK_NETCDF4_GROUP_SELF_FK                              */
/*==============================================================*/
create index FK_NETCDF4_GROUP_SELF_FK on NC4_GROUP (
   NC4_N_GROUPID ASC
);

/*==============================================================*/
/* Index: FK_N4G_N4S_FK                                         */
/*==============================================================*/
create index FK_N4G_N4S_FK on NC4_GROUP (
   PRODUCTID ASC
);

/*==============================================================*/
/* Table: NC4_GROUPATTRIBUTE                                    */
/*==============================================================*/
create table NC4_GROUPATTRIBUTE
(
   N_GROUPATTRIBUTEID   INTEGER            not null,
   N_GROUPID            INTEGER            not null,
   N_GROUPATTRIBUTENAME VARCHAR(255),
   N_GROUPATTRIBUTEDATATYPE VARCHAR(255),
   N_GROUPATTRIBUTEEXTERNALFLAG SMALLINT,
   N_GROUPATTRIBUTEDELIMITER VARCHAR(2),
   N_GROUPATTRIBUTESTRINGVALUE VARCHAR(2048),
   N_GROUPNUMATTRVALUES INTEGER,
   constraint PK_NC4_GROUPATTRIBUTE primary key (N_GROUPATTRIBUTEID)
);

/*==============================================================*/
/* Index: FK_NGA_NG_FK                                          */
/*==============================================================*/
create index FK_NGA_NG_FK on NC4_GROUPATTRIBUTE (
   N_GROUPID ASC
);

/*==============================================================*/
/* Table: NDE_OPERATORLOG                                       */
/*==============================================================*/
create table NDE_OPERATORLOG
(
   NOL_ID               BIGINT           not null,
   NDEUSERID            INTEGER            not null,
   NOLENTRYTIME         TIMESTAMP            not null,
   NOLTEXT              VARCHAR(2048),
   constraint PK_NDE_OPERATORLOG primary key (NOL_ID)
);

/*==============================================================*/
/* Index: RELATIONSHIP_129_FK                                   */
/*==============================================================*/
create index RELATIONSHIP_129_FK on NDE_OPERATORLOG (
   NDEUSERID ASC
);

/*==============================================================*/
/* Table: NDE_SUPPORTFUNCTION                                   */
/*==============================================================*/
create table NDE_SUPPORTFUNCTION
(
   NSFID                INTEGER            not null,
   NSFDESCRIPTION       VARCHAR(255),
   NSFTYPE              VARCHAR(25),
   NSFPATHORCLASSNAME   VARCHAR(255),
   NSFMETHODOREXECUTABLENAME VARCHAR(255),
   constraint PK_NDE_SUPPORTFUNCTION primary key (NSFID)
);

/*==============================================================*/
/* Table: NDE_USER                                              */
/*==============================================================*/
create table NDE_USER
(
   NDEUSERID            INTEGER            not null,
   NDEUSERPRIVILEGES    VARCHAR(255)        not null,
   NDEUSERIDENTIFIER    VARCHAR(255)        not null,
   NDEUSERPASSWORD      VARCHAR(255)        not null,
   NDEUSER_PW_LASTUPDATETIME TIMESTAMP            not null,
   constraint PK_NDE_USER primary key (NDEUSERID)
);

/*==============================================================*/
/* Index: NDE_USER_UNIQUE                                       */
/*==============================================================*/
create unique index NDE_USER_UNIQUE on NDE_USER (
   NDEUSERIDENTIFIER ASC
);

/*==============================================================*/
/* Table: NETCDF4_STRUCTURE                                     */
/*==============================================================*/
create table NETCDF4_STRUCTURE
(
   PRODUCTID            BIGINT           not null,
   constraint PK_NETCDF4_STRUCTURE primary key (PRODUCTID)
);

/*==============================================================*/
/* Table: NOTIFICATIONJOB                                       */
/*==============================================================*/
create table NOTIFICATIONJOB
(
   NOJOBID              BIGINT           not null,
   NOJOBSTATUS          VARCHAR(15)         not null,
   NOJOBENQUEUETIME     TIMESTAMP,
   NOJOBSTARTTIME       TIMESTAMP,
   NOJOBCOMPLETIONTIME  TIMESTAMP,
   constraint PK_NOTIFICATIONJOB primary key (NOJOBID)
);

/*==============================================================*/
/* Table: NOTIFICATIONREQUEST                                   */
/*==============================================================*/
create table NOTIFICATIONREQUEST
(
   NOREQID              BIGINT           not null,
   NOJOBID              BIGINT,
   DIJOBID              BIGINT           not null,
   NOREQSTATUS          VARCHAR(15)         not null,
   constraint PK_NOTIFICATIONREQUEST primary key (NOREQID)
);

/*==============================================================*/
/* Index: NOJOB_NOREQ_REF_FK                                    */
/*==============================================================*/
create index NOJOB_NOREQ_REF_FK on NOTIFICATIONREQUEST (
   NOJOBID ASC
);

/*==============================================================*/
/* Index: DIJOB_NOREQ_REF_FK                                    */
/*==============================================================*/
create index DIJOB_NOREQ_REF_FK on NOTIFICATIONREQUEST (
   DIJOBID ASC
);

/*==============================================================*/
/* Table: NOTIFYBYFILELOCK                                      */
/*==============================================================*/
create table NOTIFYBYFILELOCK
(
   LOCKCOLUMN           INTEGER
);

/*==============================================================*/
/* Table: NOTIFYBYHOURLOCK                                      */
/*==============================================================*/
create table NOTIFYBYHOURLOCK
(
   LOCKCOLUMN           INTEGER
);

/*==============================================================*/
/* Table: NOTIFYCLASSMANIFESTLOCK                               */
/*==============================================================*/
create table NOTIFYCLASSMANIFESTLOCK
(
   LOCKCOLUMN          INTEGER
);

/*==============================================================*/
/* Table: PLATFORM                                              */
/*==============================================================*/
create table PLATFORM
(
   PLATFORMID           BIGINT           not null,
   PLATFORMNAME         VARCHAR(255),
   PLATFORMMISSIONNAME  VARCHAR(255),
   constraint PK_PLATFORM primary key (PLATFORMID)
);

/*==============================================================*/
/* Table: PLATFORMACQUISITIONSCHEDULE                           */
/*==============================================================*/
create table PLATFORMACQUISITIONSCHEDULE
(
   PLATFORMID           BIGINT           not null,
   PASORBITNUMBER       INTEGER              not null,
   PASACQUISITIONTIME   TIMESTAMP            not null,
   PASLOSSTIME          TIMESTAMP            not null,
   constraint PK_PLATFORMACQUISITIONSCHEDULE primary key (PLATFORMID, PASORBITNUMBER)
);

/*==============================================================*/
/* Index: RELATIONSHIP_125_FK                                   */
/*==============================================================*/
create index RELATIONSHIP_125_FK on PLATFORMACQUISITIONSCHEDULE (
   PLATFORMID ASC
);

/*==============================================================*/
/* Table: PLATFORMORBIT                                         */
/*==============================================================*/
create table PLATFORMORBIT
(
   PLATFORMID           BIGINT           not null,
   PLORBITTYPEID        INTEGER            not null,
   PLORBITID            INTEGER              not null,
   PLORBITSTARTTIME     TIMESTAMP,
   PLORBITENDTIME       TIMESTAMP,
   constraint PK_PLATFORMORBIT primary key (PLATFORMID, PLORBITTYPEID, PLORBITID)
);

/*==============================================================*/
/* Index: RELATIONSHIP_79_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_79_FK on PLATFORMORBIT (
   PLATFORMID ASC,
   PLORBITTYPEID ASC
);

/*==============================================================*/
/* Table: PLATFORMORBITTYPE                                     */
/*==============================================================*/
create table PLATFORMORBITTYPE
(
   PLATFORMID           BIGINT           not null,
   PLORBITTYPEID        INTEGER            not null,
   PLORBITNAME          VARCHAR(255),
   constraint PK_PLATFORMORBITTYPE primary key (PLATFORMID, PLORBITTYPEID)
);

/*==============================================================*/
/* Index: PLATFORMPLATFORMORBIT_FK_FK                           */
/*==============================================================*/
create index PLATFORMPLATFORMORBIT_FK_FK on PLATFORMORBITTYPE (
   PLATFORMID ASC
);

/*==============================================================*/
/* Table: PLATFORMSENSOR                                        */
/*==============================================================*/
create table PLATFORMSENSOR
(
   PLATSENID            BIGINT           not null,
   PLATFORMID           BIGINT           not null,
   PLATSENSOR           VARCHAR(255),
   PLATMINORBITSECONDS  INTEGER,
   constraint PK_PLATFORMSENSOR primary key (PLATSENID)
);

/*==============================================================*/
/* Index: PLATFORMSENSORPLATFORM_FK_FK                          */
/*==============================================================*/
create index PLATFORMSENSORPLATFORM_FK_FK on PLATFORMSENSOR (
   PLATFORMID ASC
);

/*==============================================================*/
/* Table: POINTOFCONTACT                                        */
/*==============================================================*/
create table POINTOFCONTACT
(
   POCID                INTEGER            not null,
   POCEMAILADDR         VARCHAR(255)        not null,
   POCALTEMAILADDR      VARCHAR(255),
   POCORGANIZATION      VARCHAR(255)        not null,
   POCPHONENUMBER       VARCHAR(25)         not null,
   POCFAXNUMBER         VARCHAR(25),
   constraint PK_POINTOFCONTACT primary key (POCID)
);

/*==============================================================*/
/* Table: PRINPUTPRODUCT                                        */
/*==============================================================*/
create table PRINPUTPRODUCT
(
   PRIPID               BIGINT           not null,
   PRINPUTPREFERENCE    INTEGER              not null,
   PRISID               BIGINT           not null,
   PRODUCTID            BIGINT,
   ALGORITHMID          BIGINT,
   constraint PK_PRINPUTPRODUCT primary key (PRIPID, PRINPUTPREFERENCE)
);

/*==============================================================*/
/* Index: RELATIONSHIP_84_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_84_FK on PRINPUTPRODUCT (
   PRISID ASC
);

/*==============================================================*/
/* Index: RELATIONSHIP_85_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_85_FK on PRINPUTPRODUCT (
   PRODUCTID ASC,
   ALGORITHMID ASC
);

/*  NEW, 3/25/2016 PGM */

create index PIP_PRIS_PROD_IDX on PRINPUTPRODUCT(PRODUCTID, PRISID);


/*==============================================================*/
/* Table: PRINPUTSPEC                                           */
/*==============================================================*/
create table PRINPUTSPEC
(
   PRISID               BIGINT           not null,
   PRID                 BIGINT           not null,
   PRISNEED             VARCHAR(25),
   PRISFILEHANDLE       VARCHAR(255)        not null,
   PRISTEST             VARCHAR(2048),
   PRISLEFTOFFSETINTERVAL INTERVAL DAY TO SECOND(5),
   PRISRIGHTOFFSETINTERVAL INTERVAL DAY TO SECOND(5),
   PRISFILEACCUMULATIONTHRESHOLD INTEGER,
   PRISFILEHANDLENUMBERING CHAR(1)              not null,
   constraint PK_PRINPUTSPEC primary key (PRISID)
);

/*==============================================================*/
/* Index: RELATIONSHIP_39_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_39_FK on PRINPUTSPEC (
   PRID ASC
);

/*==============================================================*/
/* Table: PROCESSINGNODE                                        */
/*==============================================================*/
create table PROCESSINGNODE
(
   RESOURCEID           BIGINT           not null,
   constraint PK_PROCESSINGNODE primary key (RESOURCEID)
);

/*==============================================================*/
/* Table: PROCESSINGNODEJOBBOX                                  */
/*==============================================================*/
create table PROCESSINGNODEJOBBOX
(
   PNJBID               BIGINT           not null,
   JOBPRIORITY          INTEGER              not null,
   JOBCLASS             INTEGER              not null,
   RESOURCEID           BIGINT           not null,
   NUMBOXES             INTEGER              not null,
   constraint PK_PROCESSINGNODEJOBBOX primary key (PNJBID)
);

/*==============================================================*/
/* Index: RELATIONSHIP_81_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_81_FK on PROCESSINGNODEJOBBOX (
   JOBCLASS ASC
);

/*==============================================================*/
/* Index: RELATIONSHIP_82_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_82_FK on PROCESSINGNODEJOBBOX (
   JOBPRIORITY ASC
);

/*==============================================================*/
/* Index: RELATIONSHIP_120_FK                                   */
/*==============================================================*/
create index RELATIONSHIP_120_FK on PROCESSINGNODEJOBBOX (
   RESOURCEID ASC
);

/*==============================================================*/
/* Table: PRODUCTDATASOURCE                                     */
/*==============================================================*/
create table PRODUCTDATASOURCE
(
   PRODUCTID            BIGINT           not null,
   HOSTID               BIGINT           not null,
   PRODUCTPROVIDERDIRECTORY VARCHAR(255),
   PRODUCTPROVIDERIDPSSUBID VARCHAR(255),
   PRODUCTPOLLINGFREQUENCY INTEGER,
   PRODUCTPROVIDERPOLLINGRETRIES INTEGER,
   LASTUPDATETIME       TIMESTAMP,
   constraint PK_PRODUCTDATASOURCE primary key (PRODUCTID, HOSTID)
);

/*==============================================================*/
/* Index: PROD_PRODDSOURCE_REF_FK                               */
/*==============================================================*/
create index PROD_PRODDSOURCE_REF_FK on PRODUCTDATASOURCE (
   PRODUCTID ASC
);

/*==============================================================*/
/* Index: RELATIONSHIP_72_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_72_FK on PRODUCTDATASOURCE (
   HOSTID ASC
);

/*==============================================================*/
/* Table: PRODUCTDATASOURCEFILECACHE                            */
/*==============================================================*/
create table PRODUCTDATASOURCEFILECACHE
(
   PRODUCTID            BIGINT           not null,
   HOSTID               BIGINT           not null,
   SOURCEFILENAME       VARCHAR(255)        not null,
   SOURCEFILEINSERTTIME TIMESTAMP,
   constraint PK_PRODUCTDATASOURCEFILECACHE primary key (PRODUCTID, HOSTID, SOURCEFILENAME)
);

/*==============================================================*/
/* Index: RELATIONSHIP_78_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_78_FK on PRODUCTDATASOURCEFILECACHE (
   PRODUCTID ASC,
   HOSTID ASC
);

/*==============================================================*/
/* Table: PRODUCTDESCRIPTION                                    */
/*==============================================================*/
create table PRODUCTDESCRIPTION
(
   PRODUCTID            BIGINT           not null,
   INGESTINCOMINGDIRECTORYID INTEGER,
   PRO_PRODUCTID        BIGINT,
   PRODUCTGROUPID       BIGINT,
   PRODUCTSHORTNAME     VARCHAR(255),
   PRODUCTLONGNAME      VARCHAR(255),
   PRODUCTDESCRIPTION   VARCHAR(2048),
   PRODUCTMETADATALINK  VARCHAR(255),
   PRODUCTMETADATAXML   XML,
   PRODUCTPROFILELINK   VARCHAR(255),
   PRODUCTPROFILEXML    XML,
   PRODUCTTYPE          VARCHAR(50),
   PRODUCTSUBTYPE       VARCHAR(50),
   PRODUCTFILENAMEPREFIX VARCHAR(255),
   PRODUCTIDPSMNEMONIC  VARCHAR(55),
   PRODUCTAVAILABILITYDATE TIMESTAMP,
   PRODUCTCIPPRIORITY   INTEGER,
   PRODUCTSTATUS        VARCHAR(5),
   PRODUCTFILENAMEPATTERN VARCHAR(255),
   PRODUCTHOMEDIRECTORY VARCHAR(255),
   PRODUCTSPATIALAREA    geography(POLYGON, 4326),
   PRODUCTIDPSRETRANSMITLIMIT INTEGER,
   PRODUCTAREA          VARCHAR(255),
   PRODUCTESTAVGFILESIZE INTEGER,
   PRODUCTARCHIVE       SMALLINT,
   PRODUCTRETENTIONPERIODHR INTEGER,
   PRODUCTHORIZONTALRESOLUTION VARCHAR(255),
   PRODUCTVERTICALRESOLUTION VARCHAR(255),
   PRODUCTFILEFORMAT    VARCHAR(255),
   PRODUCT_TIME_COVERAGE VARCHAR(255),
   PRODUCT_MAP_PROJECTION VARCHAR(255),
   PRODUCTFILEMETADATAEXTERNAL SMALLINT,
   PRODUCTFILENAMEEXTENSION VARCHAR(15),
   PRODUCTFILENAMEMETAEXTENSION VARCHAR(15),
   PRODUCTFILEMETATEMPLATEXML XML,
   PRODUCTCOVERAGEGAPINTERVAL_DS INTERVAL DAY TO SECOND(5),
   PRODUCTOBSTIMEPATTERN VARCHAR(255),
   PRODUCTIRMESSAGEEXTENSION VARCHAR(22),
   constraint PK_PRODUCTDESCRIPTION primary key (PRODUCTID)
);

/*==============================================================*/
/* Index: RELATIONSHIP_76_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_76_FK on PRODUCTDESCRIPTION (
   INGESTINCOMINGDIRECTORYID ASC
);

/*==============================================================*/
/* Index: PRODUCTGEOLOCATIONID_FK                               */
/*==============================================================*/
create index PRODUCTGEOLOCATIONID_FK on PRODUCTDESCRIPTION (
   PRO_PRODUCTID ASC
);

/*==============================================================*/
/* Index: RELATIONSHIP_107_FK                                   */
/*==============================================================*/
create index RELATIONSHIP_107_FK on PRODUCTDESCRIPTION (
   PRODUCTGROUPID ASC
);

/*==============================================================*/
/* Table: PRODUCTGROUP                                          */
/*==============================================================*/
create table PRODUCTGROUP
(
   PRODUCTGROUPID       BIGINT            not null,
   PRODUCTGROUPNAME     VARCHAR(255),
   constraint PK_PRODUCTGROUP primary key (PRODUCTGROUPID)
);

/*==============================================================*/
/* Table: PRODUCTGROUPDATAPARTITION                             */
/*==============================================================*/
create table PRODUCTGROUPDATAPARTITION
(
   PGDATAPARTITIONID    INTEGER            not null,
   PRODUCTGROUPID       BIGINT           not null,
   PGDPOBSERVATIONSTARTTIME TIMESTAMP,
   PGDPOBSERVATIONENDTIME TIMESTAMP,
   PGDPSPATIALAREA  geography(POLYGON, 4326),
   constraint PK_PRODUCTGROUPDATAPARTITION primary key (PGDATAPARTITIONID)
);

/*==============================================================*/
/* Index: RELATIONSHIP_105_FK                                   */
/*==============================================================*/
create index RELATIONSHIP_105_FK on PRODUCTGROUPDATAPARTITION (
   PRODUCTGROUPID ASC
);

/*==============================================================*/
/* Table: PRODUCTIONJOB                                         */
/*==============================================================*/
create table PRODUCTIONJOB
(
   PRJOBID              BIGINT           not null,
   PRODPARTIALJOBID     BIGINT           not null,
   RESOURCEID           BIGINT,
   PRJOBENQUEUETIME     TIMESTAMP,
   PRJOBSTARTTIME       TIMESTAMP,
   PRJOBCOMPLETIONTIME  TIMESTAMP,
   PRJOBSTATUS          VARCHAR(35),
   PRALGORITHMRETURNCODE INTEGER,
   PRDATASELECTIONRETURNCODE INTEGER,
   PRJOBHIGHESTERRORCLASS VARCHAR(5),
   PRJOBPID             INTEGER,
   PRJOBCPU_UTIL        NUMERIC,
   PRJOBMEM_UTIL        NUMERIC,
   PRJOBIO_UTIL         NUMERIC,
   constraint PK_PRODUCTIONJOB primary key (PRJOBID)
);

/*==============================================================*/
/* Index: PRODJOBSPECPRODJOB_FK_FK                              */
/*==============================================================*/
create index PRODJOBSPECPRODJOB_FK_FK on PRODUCTIONJOB (
   PRODPARTIALJOBID ASC
);

/*==============================================================*/
/* Index: RELATIONSHIP_122_FK                                   */
/*==============================================================*/
create index RELATIONSHIP_122_FK on PRODUCTIONJOB (
   RESOURCEID ASC
);

/*==============================================================*/
/* Index: PJ_STATUS_ENQTIME                                     */
/*==============================================================*/
create index PJ_STATUS_ENQTIME on PRODUCTIONJOB (
   PRJOBSTATUS ASC,
   PRODPARTIALJOBID ASC,
   PRJOBID ASC
);

/*==============================================================*/
/* Table: PRODUCTIONJOBLOCK                                     */
/*==============================================================*/
create table PRODUCTIONJOBLOCK
(
   LOCKCOLUMN           INTEGER
);

/*==============================================================*/
/* Table: PRODUCTIONJOBLOGMESSAGES                              */
/*==============================================================*/
create table PRODUCTIONJOBLOGMESSAGES
(
   PRJOBID              BIGINT           not null,
   PRJOBLOGMESSAGELINE  VARCHAR(2048)       not null,
   PRJOBLOGMESSAGELINENUM INTEGER,
   constraint PK_PRODUCTIONJOBLOGMESSAGES primary key (PRJOBID, PRJOBLOGMESSAGELINE)
);

/*==============================================================*/
/* Index: PRODJOBLOGMESSAGES_FK_FK                              */
/*==============================================================*/
create index PRODJOBLOGMESSAGES_FK_FK on PRODUCTIONJOBLOGMESSAGES (
   PRJOBID ASC
);

/*==============================================================*/
/* Table: PRODUCTIONJOBOUTPUTFILES                              */
/*==============================================================*/
create table PRODUCTIONJOBOUTPUTFILES
(
   PRJOBID              BIGINT           not null,
   PRJOBOUTPUTFILESEQNUM INTEGER              not null,
   PRODUCTIONJOBOUTPUTFILENAME VARCHAR(255)        not null,
   constraint PK_PRODUCTIONJOBOUTPUTFILES primary key (PRJOBID, PRJOBOUTPUTFILESEQNUM)
);

/*==============================================================*/
/* Index: RELATIONSHIP_127_FK                                   */
/*==============================================================*/
create index RELATIONSHIP_127_FK on PRODUCTIONJOBOUTPUTFILES (
   PRJOBID ASC
);

/*==============================================================*/
/* Table: PRODUCTIONJOBSPEC                                     */
/*==============================================================*/
create table PRODUCTIONJOBSPEC
(
   PRODPARTIALJOBID     BIGINT           not null,
   MPOID                BIGINT,
   MOVCODE              VARCHAR(8),
   WEDID                INTEGER,
   WEID                 BIGINT,
   PRID                 BIGINT           not null,
   JOBCLASS             INTEGER              not null,
   JOBPRIORITY          INTEGER              not null,
   PJSOBSSTARTTIME      TIMESTAMP,
   PJSOBSENDTIME        TIMESTAMP,
   PJSSTARTTIME         TIMESTAMP,
   PJSORBITNUMBER       INTEGER,
   PJSCOMPLETIONSTATUS  VARCHAR(255),
   PJSTIMEOUTTIME       TIMESTAMP,
   PJSCREATERESOURCEID  BIGINT,
   PJSCOMPLETERESOURCEID BIGINT,
   constraint PK_PRODUCTIONJOBSPEC primary key (PRODPARTIALJOBID)
);

/*==============================================================*/
/* Index: PR_PRJOBSPEC_REF_FK                                   */
/*==============================================================*/
create index PR_PRJOBSPEC_REF_FK on PRODUCTIONJOBSPEC (
   PRID ASC
);

/*==============================================================*/
/* Index: RELATIONSHIP_69_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_69_FK on PRODUCTIONJOBSPEC (
   MPOID ASC,
   MOVCODE ASC,
   WEDID ASC,
   WEID ASC
);

/*==============================================================*/
/* Index: PRODJOBSIZE_FK                                        */
/*==============================================================*/
create index PRODJOBSIZE_FK on PRODUCTIONJOBSPEC (
   JOBCLASS ASC
);

/*==============================================================*/
/* Index: PRODJOBPRIORITY_FK                                    */
/*==============================================================*/
create index PRODJOBPRIORITY_FK on PRODUCTIONJOBSPEC (
   JOBPRIORITY ASC
);

/*  NEW, 3/25/2016 PGM */

/*==============================================================*/
/* Index: PJS_STATUS_OBSENDTIME                                 */
/*==============================================================*/
/* create index PJS_STATUS_OBSENDTIME on PRODUCTIONJOBSPEC (    */
/*   PJSCOMPLETIONSTATUS ASC,                                   */
/*   PJSOBSENDTIME ASC                                          */
/* );                                                           */


create index PJS_CS_ST_PPJ_PR_IDX on PRODUCTIONJOBSPEC(PJSCOMPLETIONSTATUS, PJSSTARTTIME, PRODPARTIALJOBID, PRID);

create index PJS_PRID_ST on PRODUCTIONJOBSPEC(PRID, PJSOBSSTARTTIME);


/*==============================================================*/
/* Table: PRODUCTIONJOBSPECLOCK                                 */
/*==============================================================*/
create table PRODUCTIONJOBSPECLOCK
(
   LOCKCOLUMN           INTEGER
);

/*==============================================================*/
/* Table: PRODUCTIONJOBSPECCRLOCK                                 */
/*==============================================================*/
create table PRODUCTIONJOBSPECCRLOCK
(
   LOCKCOLUMN           INTEGER
);

/*==============================================================*/
/* Table: PRODUCTIONRULE                                        */
/*==============================================================*/
create table PRODUCTIONRULE
(
   PRID                 BIGINT           not null,
   MOVCODE              VARCHAR(8),
   WEDID                INTEGER,
   JOBPRIORITY          INTEGER              not null,
   ALGORITHMID          BIGINT           not null,
   GZID                 BIGINT,
   PLATFORMID           BIGINT,
   PLORBITTYPEID        INTEGER,
   JOBCLASS             INTEGER              not null,
   PRRULENAME           VARCHAR(255)        not null,
   PRRULETYPE           VARCHAR(22)         not null,
   PRACTIVEFLAG_NSOF    SMALLINT,
   PRACTIVEFLAG_CBU     SMALLINT,
   PRTEMPORARYSPACEMB   FLOAT                not null,
   PRESTIMATEDRAM_MB    FLOAT                not null,
   PRESTIMATEDCPU       FLOAT                not null,
   PRPRODUCTCOVERAGEINTERVAL_DS INTERVAL DAY TO SECOND(5),
   PRPRODUCTCOVERAGEINTERVAL_YM INTERVAL YEAR TO MONTH,
   PRSTARTBOUNDARYTIME  TIMESTAMP,
   PRRUNINTERVAL_DS     INTERVAL DAY TO SECOND(5),
   PRRUNINTERVAL_YM     INTERVAL YEAR TO MONTH,
   PRWEATHEREVENTDISTANCEKM FLOAT,
   PRORBITSTARTBOUNDARY INTEGER,
   PRPRODUCTORBITINTERVAL INTEGER,
   PRWAITFORINPUTINTERVAL_DS INTERVAL DAY TO SECOND(5),
   PRDATASELECTIONXML   XML,
   PRWAITFORINPUTINTERVAL_YM INTERVAL DAY TO SECOND(5),
   PRNOTIFYOPSSECONDS   INTEGER,
   constraint PK_PRODUCTIONRULE primary key (PRID)
);

/*==============================================================*/
/* Index: RELATIONSHIP_24_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_24_FK on PRODUCTIONRULE (
   ALGORITHMID ASC
);

/*==============================================================*/
/* Index: PRODUCTIONRULEWEATHEREVENT_FK                         */
/*==============================================================*/
create index PRODUCTIONRULEWEATHEREVENT_FK on PRODUCTIONRULE (
   MOVCODE ASC,
   WEDID ASC
);

/*==============================================================*/
/* Index: PRODRULEPRIORITY_FK                                   */
/*==============================================================*/
create index PRODRULEPRIORITY_FK on PRODUCTIONRULE (
   JOBPRIORITY ASC
);

/*==============================================================*/
/* Index: PRODRULEJOBSIZE_FK                                    */
/*==============================================================*/
create index PRODRULEJOBSIZE_FK on PRODUCTIONRULE (
   JOBCLASS ASC
);

/*==============================================================*/
/* Index: RELATIONSHIP_83_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_83_FK on PRODUCTIONRULE (
   GZID ASC
);

/*==============================================================*/
/* Index: RELATIONSHIP_80_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_80_FK on PRODUCTIONRULE (
   PLATFORMID ASC,
   PLORBITTYPEID ASC
);

/*==============================================================*/
/* Table: PRODUCTPLATFORM_XREF                                  */
/*==============================================================*/
create table PRODUCTPLATFORM_XREF
(
   PLATFORMID           BIGINT           not null,
   PRODUCTID            BIGINT           not null,
   constraint PK_PRODUCTPLATFORM_XREF primary key (PLATFORMID, PRODUCTID)
);

/*==============================================================*/
/* Index: PRODUCTPLATFORM_XREF_FK                               */
/*==============================================================*/
create index PRODUCTPLATFORM_XREF_FK on PRODUCTPLATFORM_XREF (
   PLATFORMID ASC
);

/*==============================================================*/
/* Index: PRODUCTPLATFORM_XREF2_FK                              */
/*==============================================================*/
create index PRODUCTPLATFORM_XREF2_FK on PRODUCTPLATFORM_XREF (
   PRODUCTID ASC
);

/*==============================================================*/
/* Table: PRODUCTQUALITYSUMMARY                                 */
/*==============================================================*/
create table PRODUCTQUALITYSUMMARY
(
   PRODUCTID            BIGINT           not null,
   PRODUCTQUALITYSUMMARYNAME VARCHAR(255)        not null,
   PRODUCTQUALITYSUMMARYTYPE VARCHAR(15),
   PRODUCTQUALITYDESCRIPTION VARCHAR(255),
   constraint PK_PRODUCTQUALITYSUMMARY primary key (PRODUCTID, PRODUCTQUALITYSUMMARYNAME)
);

/*==============================================================*/
/* Index: RELATIONSHIP_1_FK                                     */
/*==============================================================*/
create index RELATIONSHIP_1_FK on PRODUCTQUALITYSUMMARY (
   PRODUCTID ASC
);

/*==============================================================*/
/* Table: PROUTPUTSPEC                                          */
/*==============================================================*/
create table PROUTPUTSPEC
(
   PROSID               BIGINT           not null,
   PRID                 BIGINT           not null,
   PRODUCTID            BIGINT           not null,
   ALGORITHMID          BIGINT           not null,
   constraint PK_PROUTPUTSPEC primary key (PROSID)
);

/*==============================================================*/
/* Index: RELATIONSHIP_41_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_41_FK on PROUTPUTSPEC (
   PRID ASC
);

/*==============================================================*/
/* Index: RELATIONSHIP_57_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_57_FK on PROUTPUTSPEC (
   PRODUCTID ASC,
   ALGORITHMID ASC
);

/*==============================================================*/
/* Table: PRPARAMETER                                           */
/*==============================================================*/
create table PRPARAMETER
(
   PRPARAMETERSEQNO     BIGINT           not null,
   ALGOPARAMETERID      BIGINT           not null,
   PRID                 BIGINT           not null,
   PRPARAMETERVALUE     VARCHAR(255),
   constraint PK_PRPARAMETER primary key (PRPARAMETERSEQNO)
);

/*==============================================================*/
/* Index: RELATIONSHIP_37_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_37_FK on PRPARAMETER (
   PRID ASC
);

/*==============================================================*/
/* Index: RELATIONSHIP_59_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_59_FK on PRPARAMETER (
   ALGOPARAMETERID ASC
);

/*==============================================================*/
/* Table: RESOURCEINSTANCE                                      */
/*==============================================================*/
create table RESOURCEINSTANCE
(
   RESOURCEID           BIGINT           not null,
   RESOURCETYPE         VARCHAR(32)         not null,
   RESOURCELABEL        VARCHAR(255)        not null,
   constraint PK_RESOURCEINSTANCE primary key (RESOURCEID)
);

/*==============================================================*/
/* Table: RESOURCELOGSUMMARY                                    */
/*==============================================================*/
create table RESOURCELOGSUMMARY
(
   RESOURCEID           BIGINT           not null,
   RMONITORID           INTEGER            not null,
   RLSSTARTTIME         TIMESTAMP            not null,
   RLSENDTIME           TIMESTAMP            not null,
   RLSMESSAGELEVEL      VARCHAR(32),
   RLSMESSAGECATEGORY   VARCHAR(255),
   RLSMESSAGE           VARCHAR(1024)       not null,
   RLSMESSAGECOUNT      INTEGER              not null,
   constraint PK_RESOURCELOGSUMMARY primary key (RESOURCEID, RMONITORID, RLSSTARTTIME, RLSENDTIME)
);

/*==============================================================*/
/* Index: RM_RSL_REF_FK                                         */
/*==============================================================*/
create index RM_RSL_REF_FK on RESOURCELOGSUMMARY (
   RESOURCEID ASC,
   RMONITORID ASC
);

/*==============================================================*/
/* Table: RESOURCEMEASUREMENT                                   */
/*==============================================================*/
create table RESOURCEMEASUREMENT
(
   RESOURCEID           BIGINT           not null,
   RMONITORID           INTEGER            not null,
   RMEASUREMENTTIME     TIMESTAMP            not null,
   RMEASUREMENTVALUE    VARCHAR(255),
   constraint PK_RESOURCEMEASUREMENT primary key (RESOURCEID, RMONITORID, RMEASUREMENTTIME)
);

/*==============================================================*/
/* Index: RMON_RMEAS_REF_FK                                     */
/*==============================================================*/
create index RMON_RMEAS_REF_FK on RESOURCEMEASUREMENT (
   RESOURCEID ASC,
   RMONITORID ASC
);

/*==============================================================*/
/* Table: RESOURCEMONITOR                                       */
/*==============================================================*/
create table RESOURCEMONITOR
(
   RESOURCEID           BIGINT           not null,
   RMONITORID           INTEGER            not null,
   RMONITORTYPE         CHAR(1)              not null,
   RMONITORFREQMIN      INTEGER              not null,
   RMONITOROPTIONS1     VARCHAR(1024),
   RMONITOROPTIONS2     VARCHAR(1024),
   RMONITORACTIVEFLAG   SMALLINT             not null,
   constraint PK_RESOURCEMONITOR primary key (RESOURCEID, RMONITORID)
);

/*==============================================================*/
/* Index: R_RM_REF_FK                                           */
/*==============================================================*/
create index R_RM_REF_FK on RESOURCEMONITOR (
   RESOURCEID ASC
);

/*==============================================================*/
/* Table: RESOURCEMONITORALERT                                  */
/*==============================================================*/
create table RESOURCEMONITORALERT
(
   RMAID                INTEGER            not null,
   RESOURCEID           BIGINT           not null,
   RMONITORID           INTEGER            not null,
   RMAWARNTHRESHOLD     INTEGER,
   RMACRITICALTHRESHOLD INTEGER,
   RMANOTIFYTHRESHOLD   INTEGER,
   RMATHRESHOLDUNIT     VARCHAR(12),
   constraint PK_RESOURCEMONITORALERT primary key (RMAID)
);

/*==============================================================*/
/* Index: RM_RMA_REF_FK                                         */
/*==============================================================*/
create index RM_RMA_REF_FK on RESOURCEMONITORALERT (
   RESOURCEID ASC,
   RMONITORID ASC
);

/*==============================================================*/
/* Table: RESOURCEPLATFORM                                      */
/*==============================================================*/
create table RESOURCEPLATFORM
(
   RESOURCEID           BIGINT           not null,
   RPLATFORMID          BIGINT           not null,
   RPLATFORMNAME        VARCHAR(255)        not null,
   RPLATFORMTYPE        VARCHAR(255)        not null,
   constraint PK_RESOURCEPLATFORM primary key (RESOURCEID, RPLATFORMID)
);

/*==============================================================*/
/* Index: RP_R_REF_FK                                           */
/*==============================================================*/
create index RP_R_REF_FK on RESOURCEPLATFORM (
   RESOURCEID ASC
);

/*==============================================================*/
/* Table: RESOURCESERVER                                        */
/*==============================================================*/
create table RESOURCESERVER
(
   RESOURCEID           BIGINT           not null,
   RSERVERID            BIGINT           not null,
   RES_RESOURCEID       BIGINT           not null,
   RPLATFORMID          BIGINT           not null,
   RSERVERNAME          VARCHAR(255)        not null,
   RSERVERLOGFILE       VARCHAR(1024),
   RSERVERTYPE          VARCHAR(255),
   constraint PK_RESOURCESERVER primary key (RESOURCEID, RSERVERID)
);

/*==============================================================*/
/* Index: RP_RS_REF_FK                                          */
/*==============================================================*/
create index RP_RS_REF_FK on RESOURCESERVER (
   RES_RESOURCEID ASC,
   RPLATFORMID ASC
);

/*==============================================================*/
/* Index: RP_R_REF2_FK                                          */
/*==============================================================*/
create index RP_R_REF2_FK on RESOURCESERVER (
   RESOURCEID ASC
);

/*==============================================================*/
/* Table: RESOURCESERVICE                                       */
/*==============================================================*/
create table RESOURCESERVICE
(
   RESOURCEID           BIGINT           not null,
   RSERVICEID           BIGINT           not null,
   RES_RESOURCEID       BIGINT           not null,
   RSERVERID            BIGINT           not null,
   RSERVICENAME         VARCHAR(255)        not null,
   RSERVICETYPE         VARCHAR(255)        not null,
   constraint PK_RESOURCESERVICE primary key (RESOURCEID, RSERVICEID)
);

/*==============================================================*/
/* Index: RSERVER_RSERVICE_REF_FK                               */
/*==============================================================*/
create index RSERVER_RSERVICE_REF_FK on RESOURCESERVICE (
   RES_RESOURCEID ASC,
   RSERVERID ASC
);

/*==============================================================*/
/* Index: RP_R_REF3_FK                                          */
/*==============================================================*/
create index RP_R_REF3_FK on RESOURCESERVICE (
   RESOURCEID ASC
);

/*==============================================================*/
/* Table: SUBAPPROVALEVENTS                                     */
/*==============================================================*/
create table SUBAPPROVALEVENTS
(
   SUBAPPROVALEVENTID   INTEGER            not null,
   SUBAPPROVALEVENTTYPE INTEGER,
   constraint PK_SUBAPPROVALEVENTS primary key (SUBAPPROVALEVENTID)
);

/*==============================================================*/
/* Table: SUBAPPROVALLOG                                        */
/*==============================================================*/
create table SUBAPPROVALLOG
(
   SUBSCRIPTIONID       INTEGER            not null,
   SUBAPPROVALEVENTID   INTEGER            not null,
   SUBAPPROVALEVENTTIME TIMESTAMP,
   SUBAPPROVALCOMMENT   varchar(255),
   constraint PK_SUBAPPROVALLOG primary key (SUBSCRIPTIONID, SUBAPPROVALEVENTID)
);

/*==============================================================*/
/* Index: RELATIONSHIP_19_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_19_FK on SUBAPPROVALLOG (
   SUBSCRIPTIONID ASC
);

/*==============================================================*/
/* Index: RELATIONSHIP_20_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_20_FK on SUBAPPROVALLOG (
   SUBAPPROVALEVENTID ASC
);

/*==============================================================*/
/* Table: SUBNOTIFICATIONTYPE                                   */
/*==============================================================*/
create table SUBNOTIFICATIONTYPE
(
   DELIVERYNOTIFICATIONTYPEID BIGINT           not null,
   SUBSCRIPTIONID       INTEGER            not null,
   HOSTID               BIGINT,
   DELIVERYTYPEOPTIONS  VARCHAR(255),
   NOTIFICATIONADDRESS  VARCHAR(255),
   DELIVERYFREQUENCYVALUE INTEGER,
   DELIVERYFREQUENCYUNIT VARCHAR(12),
   DELIVERYDDRDIRECTORY VARCHAR(255),
   constraint PK_SUBNOTIFICATIONTYPE primary key (DELIVERYNOTIFICATIONTYPEID, SUBSCRIPTIONID)
);

/*==============================================================*/
/* Index: RELATIONSHIP_15_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_15_FK on SUBNOTIFICATIONTYPE (
   DELIVERYNOTIFICATIONTYPEID ASC
);

/*==============================================================*/
/* Index: RELATIONSHIP_16_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_16_FK on SUBNOTIFICATIONTYPE (
   SUBSCRIPTIONID ASC
);

/*==============================================================*/
/* Index: RELATIONSHIP_32_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_32_FK on SUBNOTIFICATIONTYPE (
   HOSTID ASC
);

/*==============================================================*/
/* Table: SUBSCRIPTION                                          */
/*==============================================================*/
create table SUBSCRIPTION
(
   SUBSCRIPTIONID       INTEGER            not null,
   DIPRIORITY           INTEGER            not null,
   PRODUCTID            BIGINT           not null,
   SUBPRIMARYHOSTID     BIGINT,
   CUSTPROFILEID        BIGINT           not null,
   DICLASS              INTEGER            not null,
   SUBOBSSTARTTIME      TIMESTAMP,
   SUBOBSENDTIME        TIMESTAMP,
   SUBDELIVERYTIMELINESSMIN INTEGER,
   SUBCOMPRESSIONTYPE   VARCHAR(25),
   SUBDELIVERYTYPE      VARCHAR(4),
   SUBCOMMENT           VARCHAR(1024),
   SUBESTIMATEDFILESPERDAY INTEGER,
   SUBESTIMATEDTRANSFERPERDAY INTEGER,
   SUBESTIMATEDMBPERDAY INTEGER,
   SUBSTATUS            VARCHAR(15),
   SUBACTIVE            SMALLINT             not null,
   SUBLATENCY           VARCHAR(12),
   SUBSPATIALAREA       geography(POLYGON, 4326),
   SUBFORARCHIVE        SMALLINT,
   SUBCHECKSUMTYPE      VARCHAR(12),
   SUBTEST              VARCHAR(2048),
   SUBPRIMARYPUSHDIR    VARCHAR(255),
   SUBSECONDARYHOSTID   BIGINT,
   SUBSECONDARYPUSHDIR  VARCHAR(255),
   SUBFILENAMEPREFIXFER VARCHAR(255),
   SUBFILENAMESUFFITXFER VARCHAR(255),
   constraint PK_SUBSCRIPTION primary key (SUBSCRIPTIONID)
);

/*==============================================================*/
/* Index: RELATIONSHIP_13_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_13_FK on SUBSCRIPTION (
   PRODUCTID ASC
);

/*==============================================================*/
/* Index: RELATIONSHIP_14_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_14_FK on SUBSCRIPTION (
   CUSTPROFILEID ASC
);

/*==============================================================*/
/* Index: RELATIONSHIP_114_FK                                   */
/*==============================================================*/
create index RELATIONSHIP_114_FK on SUBSCRIPTION (
   SUBPRIMARYHOSTID ASC
);

/*==============================================================*/
/* Index: RELATIONSHIP_117_FK                                   */
/*==============================================================*/
create index RELATIONSHIP_117_FK on SUBSCRIPTION (
   DICLASS ASC
);

/*==============================================================*/
/* Index: RELATIONSHIP_118_FK                                   */
/*==============================================================*/
create index RELATIONSHIP_118_FK on SUBSCRIPTION (
   DIPRIORITY ASC
);

/*==============================================================*/
/* Table: SYSTEMNOTICE                                          */
/*==============================================================*/
create table SYSTEMNOTICE
(
   SYSTEMNOTICEID       INTEGER            not null,
   NDEUSERID            INTEGER,
   SNTIMESTART          TIMESTAMP            not null,
   SNTIMEEND            TIMESTAMP,
   SNINTERNALFLAG       SMALLINT             not null,
   SNCUSTOMERFLAG       SMALLINT             not null,
   SNDESCRIPTION        VARCHAR(1024)       not null,
   SITITLE              VARCHAR(255)        not null,
   constraint PK_SYSTEMNOTICE primary key (SYSTEMNOTICEID)
);

/*==============================================================*/
/* Index: NDE_USER_SYSNOTICE_FK                                 */
/*==============================================================*/
create index NDE_USER_SYSNOTICE_FK on SYSTEMNOTICE (
   NDEUSERID ASC
);

/*==============================================================*/
/* Table: WEATHEREVENT                                          */
/*==============================================================*/
create table WEATHEREVENT
(
   MPOID                BIGINT           not null,
   MOVCODE              VARCHAR(8)          not null,
   WEDID                INTEGER            not null,
   WEID                 BIGINT           not null,
   WEDETECTEDTIME       TIMESTAMP,
   constraint PK_WEATHEREVENT primary key (MPOID, MOVCODE, WEDID, WEID)
);

/*==============================================================*/
/* Index: RELATIONSHIP_63_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_63_FK on WEATHEREVENT (
   MPOID ASC
);

/*==============================================================*/
/* Index: RELATIONSHIP_64_FK                                    */
/*==============================================================*/
create index RELATIONSHIP_64_FK on WEATHEREVENT (
   MOVCODE ASC,
   WEDID ASC
);

/*==============================================================*/
/* Table: WEATHEREVENTDEFINITION                                */
/*==============================================================*/
create table WEATHEREVENTDEFINITION
(
   MOVCODE              VARCHAR(8)          not null,
   WEDID              BIGINT            not null,
   WEDNAME              VARCHAR(255),
   WEDDATATYPE          CHAR(12),
   WEDMINVALUE          FLOAT,
   WEDMAXVALUE          FLOAT,
   WEDPATTERN           VARCHAR(255),
   constraint PK_WEATHEREVENTDEFINITION primary key (MOVCODE, WEDID)
);

/*==============================================================*/
/* Index: WED_MOV_BMC_REF_FK                                    */
/*==============================================================*/
create index WED_MOV_BMC_REF_FK on WEATHEREVENTDEFINITION (
   MOVCODE ASC
);

alter table ALGOINPUTPROD
   add constraint FK_ALGOINPU_RELATIONS_PRODUCTD foreign key (PRODUCTID)
      references PRODUCTDESCRIPTION (PRODUCTID);

alter table ALGOINPUTPROD
   add constraint FK_ALGOINPU_RELATIONS_ALGORITH foreign key (ALGORITHMID)
      references ALGORITHM (ALGORITHMID);

alter table ALGOOUTPUTPROD
   add constraint FK_ALGOOUTP_RELATIONS_PRODUCTD foreign key (PRODUCTID)
      references PRODUCTDESCRIPTION (PRODUCTID);

alter table ALGOOUTPUTPROD
   add constraint FK_ALGOOUTP_RELATIONS_ALGORITH foreign key (ALGORITHMID)
      references ALGORITHM (ALGORITHMID);

alter table ALGOPARAMETERS
   add constraint FK_ALGOPARA_RELATIONS_ALGORITH foreign key (ALGORITHMID)
      references ALGORITHM (ALGORITHMID);

alter table ALGOPROCESSINGNODE_XREF
   add constraint FK_ALGOPROC_ALGOPROCE_PROCESSI foreign key (RESOURCEID)
      references PROCESSINGNODE (RESOURCEID);

alter table ALGOPROCESSINGNODE_XREF
   add constraint FK_ALGOPROC_ALGOPROCE_ALGORITH foreign key (ALGORITHMID)
      references ALGORITHM (ALGORITHMID);

alter table CUSTOMERPOINTSOFCONTACT
   add constraint FK_CUSTOMER_RELATIONS_POINTOFC foreign key (POCID)
      references POINTOFCONTACT (POCID);

alter table CUSTOMERPOINTSOFCONTACT
   add constraint FK_CUSTOMER_RELATIONS_CUSTOMER foreign key (CUSTPROFILEID)
      references CUSTOMERPROFILE (CUSTPROFILEID);

alter table DISPULLJOBCOMPLETIONTIME
   add constraint FK_DISPULLJ_RELATIONS_DISTRIBU foreign key (DIJOBID)
      references DISTRIBUTIONJOB (DIJOBID);

alter table DISTRIBUTIONJOB
   add constraint FK_DISTRIBU_DINODE_DI_DISTRIBU foreign key (RESOURCEID)
      references DISTRIBUTIONNODE (RESOURCEID);

alter table DISTRIBUTIONJOB
   add constraint FK_DISTRIBU_DIREQ_DIJ_DISTRIBU foreign key (DIREQID)
      references DISTRIBUTIONREQUEST (DIREQID);

alter table DISTRIBUTIONJOBEXTERNALHOST
   add constraint FK_DISTRIBU_DIJEH_DN__DISTRIBU foreign key (RESOURCEID)
      references DISTRIBUTIONNODE (RESOURCEID);

alter table DISTRIBUTIONJOBEXTERNALHOST
   add constraint FK_DISTRIBU_DJ_DJEH_R_DISTRIBU foreign key (DIJOBID)
      references DISTRIBUTIONJOB (DIJOBID);

alter table DISTRIBUTIONNODE
   add constraint FK_DISTRIBU_INHERITAN_RESOURCE foreign key (RESOURCEID)
      references RESOURCEINSTANCE (RESOURCEID);

alter table DISTRIBUTIONNODEJOBBOX
   add constraint FK_DISTRIBU_DIJBDIJOB_DISJOBCL foreign key (DICLASS)
      references DISJOBCLASSCODE (DICLASS);

alter table DISTRIBUTIONNODEJOBBOX
   add constraint FK_DISTRIBU_DIJBDIJOB_DISJOBPR foreign key (DIPRIORITY)
      references DISJOBPRIORITYCODE (DIPRIORITY);

alter table DISTRIBUTIONNODEJOBBOX
   add constraint FK_DISTRIBU_DISTRIBUT_DISTRIBU foreign key (RESOURCEID)
      references DISTRIBUTIONNODE (RESOURCEID);

alter table DISTRIBUTIONPREPAREREQUEST
   add constraint FK_DISTRIBU_DPR_FILEM_FILEMETA foreign key (FILEID)
      references FILEMETADATA (FILEID);

alter table DISTRIBUTIONREQUEST
   add constraint FK_DISTRIBU_DIREQDIJO_DISJOBCL foreign key (DICLASS)
      references DISJOBCLASSCODE (DICLASS);

alter table DISTRIBUTIONREQUEST
   add constraint FK_DISTRIBU_DIREQDIJO_DISJOBPR foreign key (DIPRIORITY)
      references DISJOBPRIORITYCODE (DIPRIORITY);

alter table DISTRIBUTIONREQUEST
   add constraint FK_DISTRIBU_DR_DPR_XR_DISTRIBU foreign key (DPRREQID)
      references DISTRIBUTIONPREPAREREQUEST (DPRREQID);

alter table DISTRIBUTIONREQUEST
   add constraint FK_DISTRIBU_SUB_DIREQ_SUBSCRIP foreign key (SUBSCRIPTIONID)
      references SUBSCRIPTION (SUBSCRIPTIONID);

alter table ENTERPRISEMEASURE
   add constraint FK_ENTERPRI_FK_EM_EDL_ENTERPRI foreign key (E_DIMENSIONLISTID)
      references ENTERPRISEDIMENSIONLIST (E_DIMENSIONLISTID);

alter table ENTERPRISEORDEREDDIMENSION
   add constraint FK_ENTERPRI_FK_EOD_ED_ENTERPRI foreign key (E_DIMENSIONLISTID)
      references ENTERPRISEDIMENSIONLIST (E_DIMENSIONLISTID);

alter table ENTERPRISEORDEREDDIMENSION
   add constraint FK_ENTERPRI_FK_EOD_EN_ENTERPRI foreign key (E_DIMENSIONID)
      references ENTERPRISEDIMENSION (E_DIMENSIONID);

alter table FILEMETADATA
   add constraint FK_FILEMETA_RELATIONS_PRODUCTG foreign key (PGDATAPARTITIONID)
      references PRODUCTGROUPDATAPARTITION (PGDATAPARTITIONID);

alter table FILEMETADATA
   add constraint FK_FILEMETA_RELATIONS_PRODUCTD foreign key (PRODUCTID)
      references PRODUCTDESCRIPTION (PRODUCTID);

alter table FILEQUALITYSUMMARY
   add constraint FK_FILEQUAL_RELATIONS_PRODUCTQ foreign key (PRODUCTID, PRODUCTQUALITYSUMMARYNAME)
      references PRODUCTQUALITYSUMMARY (PRODUCTID, PRODUCTQUALITYSUMMARYNAME);

alter table FILEQUALITYSUMMARY
   add constraint FK_FILEQUAL_RELATIONS_FILEMETA foreign key (FILEID)
      references FILEMETADATA (FILEID);

alter table FILERETRANSMITCOUNT
   add constraint FK_FILERETR_RELATIONS_PRODUCTD foreign key (PRODUCTID)
      references PRODUCTDESCRIPTION (PRODUCTID);

alter table FILERETRYHISTORY
   add constraint FK_FILERETR_RELATIONS_FILERETR foreign key (BADFILENAME)
      references FILERETRANSMITCOUNT (BADFILENAME);

alter table HDF5_ARRAY
   add constraint FK_HDF5_ARR_RELATIONS_HDF5_GRO foreign key (H_GROUPID)
      references HDF5_GROUP (H_GROUPID);

alter table HDF5_ARRAYATTRIBUTE
   add constraint FK_HDF5_ARR_FK_H5AA_H_HDF5_ARR foreign key (H_ARRAYID)
      references HDF5_ARRAY (H_ARRAYID);

alter table HDF5_DIMENSIONLIST
   add constraint FK_HDF5_DIM_RELATIONS_HDF5_ARR foreign key (H_ARRAYID)
      references HDF5_ARRAY (H_ARRAYID);

alter table HDF5_GROUP
   add constraint FK_HDF5_GRO_FK_HDF5_G_HDF5_GRO foreign key (HDF_H_GROUPID)
      references HDF5_GROUP (H_GROUPID);

alter table HDF5_GROUP
   add constraint FK_HDF5_GRO_RELATIONS_HDF5_STR foreign key (PRODUCTID)
      references HDF5_STRUCTURE (PRODUCTID);

alter table HDF5_GROUPATTRIBUTE
   add constraint FK_HDF5_GRO_RELATIONS_HDF5_GRO foreign key (H_GROUPID)
      references HDF5_GROUP (H_GROUPID);

alter table HDF5_STRUCTURE
   add constraint FK_HDF5_STR_RELATIONS_PRODUCTD foreign key (PRODUCTID)
      references PRODUCTDESCRIPTION (PRODUCTID);

alter table INGESTPROCESSSTEP
   add constraint FK_INGESTPR_RELATIONS_PRODUCTD foreign key (PRODUCTID)
      references PRODUCTDESCRIPTION (PRODUCTID);

alter table INGESTPROCESSSTEP
   add constraint FK_INGESTPR_RELATIONS_NDE_SUPP foreign key (NSFID)
      references NDE_SUPPORTFUNCTION (NSFID);

alter table INGESTREQUESTBUFFER
   add constraint FK_INGESTRE_RELATIONS_INGESTRE foreign key (IR_ID)
      references INGESTREQUESTLOG (IR_ID);

alter table INGESTREQUESTLOG
   add constraint FK_INGESTRE_RELATIONS_PRODUCTD foreign key (PRODUCTID)
      references PRODUCTDESCRIPTION (PRODUCTID);

alter table INGESTREQUESTLOG
   add constraint FK_INGESTRE_RELATIONS_INGESTIN foreign key (INGESTINCOMINGDIRECTORYID)
      references INGESTINCOMINGDIRECTORY (INGESTINCOMINGDIRECTORYID);

alter table JOBSPECINPUT
   add constraint FK_JOBSPECI_RELATIONS_PRODUCTI foreign key (PRODPARTIALJOBID)
      references PRODUCTIONJOBSPEC (PRODPARTIALJOBID);

alter table JOBSPECINPUT
   add constraint FK_JOBSPECI_RELATIONS_PRINPUTP foreign key (PRIPID, PRINPUTPREFERENCE)
      references PRINPUTPRODUCT (PRIPID, PRINPUTPREFERENCE);

alter table JOBSPECINPUT
   add constraint FK_JOBSPECI_RELATIONS_FILEMETA foreign key (FILEID)
      references FILEMETADATA (FILEID);

alter table JOBSPECOUTPUT
   add constraint FK_JOBSPECO_RELATIONS_PRODUCTI foreign key (PRODPARTIALJOBID)
      references PRODUCTIONJOBSPEC (PRODPARTIALJOBID);

alter table JOBSPECOUTPUT
   add constraint FK_JOBSPECO_RELATIONS_PROUTPUT foreign key (PROSID)
      references PROUTPUTSPEC (PROSID);

alter table JOBSPECPARAMETERS
   add constraint FK_JOBSPECP_RELATIONS_PRODUCTI foreign key (PRODPARTIALJOBID)
      references PRODUCTIONJOBSPEC (PRODPARTIALJOBID);

alter table JOBSPECPARAMETERS
   add constraint FK_JOBSPECP_RELATIONS_PRPARAME foreign key (PRPARAMETERSEQNO)
      references PRPARAMETER (PRPARAMETERSEQNO);

alter table MEASURE_H_ARRAY_XREF
   add constraint FK_MEASURE__FK_H_ARRA_HDF5_ARR foreign key (H_ARRAYID)
      references HDF5_ARRAY (H_ARRAYID);

alter table MEASURE_H_ARRAY_XREF
   add constraint FK_MEASURE__FK_MHAX_M_ENTERPRI foreign key (MEASUREID)
      references ENTERPRISEMEASURE (MEASUREID);

alter table MEASURE_N_ARRAY_XREF
   add constraint FK_MEASURE__FK_MNAX_M_ENTERPRI foreign key (MEASUREID)
      references ENTERPRISEMEASURE (MEASUREID);

alter table MEASURE_N_ARRAY_XREF
   add constraint FK_MEASURE__RELATIONS_NC4_ARRA foreign key (N_ARRAYID)
      references NC4_ARRAY (N_ARRAYID);

alter table METEOPOINTOBS
   add constraint FK_METEOPOI_RELATIONS_METEOSTA foreign key (MSBLOCKNUMBER, MSSTATIONNUMBER)
      references METEOSTATION (MSBLOCKNUMBER, MSSTATIONNUMBER);

alter table METEOPOINTOBS
   add constraint FK_METEOPOI_RELATIONS_METEOOBS foreign key (MOVCODE)
      references METEOOBSVARIABLES (MOVCODE);

alter table MHA_SUBSET
   add constraint FK_MHA_SUBS_RELATIONS_MEASURE_ foreign key (MEASUREID, H_ARRAYID)
      references MEASURE_H_ARRAY_XREF (MEASUREID, H_ARRAYID);

alter table MNA_SUBSET
   add constraint FK_MNA_SUBS_RELATIONS_MEASURE_ foreign key (N_ARRAYID, MEASUREID)
      references MEASURE_N_ARRAY_XREF (N_ARRAYID, MEASUREID);

alter table NC4_ARRAY
   add constraint FK_NC4_ARRA_FK_NA_NG_NC4_GROU foreign key (N_GROUPID)
      references NC4_GROUP (N_GROUPID);

alter table NC4_ARRAYATTRIBUTE
   add constraint FK_NC4_ARRA_FK_N4AA_N_NC4_ARRA foreign key (N_ARRAYID)
      references NC4_ARRAY (N_ARRAYID);

alter table NC4_DIMENSION
   add constraint FK_NC4_DIME_FK_N4D_N4_NC4_GROU foreign key (N_GROUPID)
      references NC4_GROUP (N_GROUPID);

alter table NC4_DIMENSIONLIST
   add constraint FK_NC4_DIME_FK_N4_DIM_NC4_DIME foreign key (N_DIMENSIONID)
      references NC4_DIMENSION (N_DIMENSIONID);

alter table NC4_DIMENSIONLIST
   add constraint FK_NC4_DIME_FK_NDL_NA_NC4_ARRA foreign key (N_ARRAYID)
      references NC4_ARRAY (N_ARRAYID);

alter table NC4_GROUP
   add constraint FK_NC4_GROU_FK_N4G_N4_NETCDF4_ foreign key (PRODUCTID)
      references NETCDF4_STRUCTURE (PRODUCTID);

alter table NC4_GROUP
   add constraint FK_NC4_GROU_FK_NETCDF_NC4_GROU foreign key (NC4_N_GROUPID)
      references NC4_GROUP (N_GROUPID);

alter table NC4_GROUPATTRIBUTE
   add constraint FK_NC4_GROU_FK_NGA_NG_NC4_GROU foreign key (N_GROUPID)
      references NC4_GROUP (N_GROUPID);

alter table NDE_OPERATORLOG
   add constraint FK_NDE_OPER_RELATIONS_NDE_USER foreign key (NDEUSERID)
      references NDE_USER (NDEUSERID);

alter table NETCDF4_STRUCTURE
   add constraint FK_NETCDF4__RELATIONS_PRODUCTD foreign key (PRODUCTID)
      references PRODUCTDESCRIPTION (PRODUCTID);

alter table NOTIFICATIONREQUEST
   add constraint FK_NOTIFICA_DIJOB_NOR_DISTRIBU foreign key (DIJOBID)
      references DISTRIBUTIONJOB (DIJOBID);

alter table NOTIFICATIONREQUEST
   add constraint FK_NOTIFICA_NOJOB_NOR_NOTIFICA foreign key (NOJOBID)
      references NOTIFICATIONJOB (NOJOBID);

alter table PLATFORMACQUISITIONSCHEDULE
   add constraint FK_PLATFORM_PLATFROM__PLATFORM foreign key (PLATFORMID)
      references PLATFORM (PLATFORMID);

alter table PLATFORMORBIT
   add constraint FK_PLATFORM_RELATIONS_PLATFORM foreign key (PLATFORMID, PLORBITTYPEID)
      references PLATFORMORBITTYPE (PLATFORMID, PLORBITTYPEID);

alter table PLATFORMORBITTYPE
   add constraint FK_PLATFORM_PLATFORMP_PLATFORM foreign key (PLATFORMID)
      references PLATFORM (PLATFORMID);

alter table PLATFORMSENSOR
   add constraint FK_PLATFORM_PLATFORMS_PLATFORM foreign key (PLATFORMID)
      references PLATFORM (PLATFORMID);

alter table PRINPUTPRODUCT
   add constraint FK_PRINPUTP_RELATIONS_PRINPUTS foreign key (PRISID)
      references PRINPUTSPEC (PRISID);

alter table PRINPUTPRODUCT
   add constraint FK_PRINPUTP_RELATIONS_ALGOINPU foreign key (PRODUCTID, ALGORITHMID)
      references ALGOINPUTPROD (PRODUCTID, ALGORITHMID);

alter table PRINPUTSPEC
   add constraint FK_PRINPUTS_RELATIONS_PRODUCTI foreign key (PRID)
      references PRODUCTIONRULE (PRID);

alter table PROCESSINGNODE
   add constraint FK_PROCESSI_INHERITAN_RESOURCE foreign key (RESOURCEID)
      references RESOURCEINSTANCE (RESOURCEID);

alter table PROCESSINGNODEJOBBOX
   add constraint FK_PROCESSI_RELATIONS_PROCESSI foreign key (RESOURCEID)
      references PROCESSINGNODE (RESOURCEID);

alter table PROCESSINGNODEJOBBOX
   add constraint FK_PROCESSI_RELATIONS_JOBCLASS foreign key (JOBCLASS)
      references JOBCLASSCODE (JOBCLASS);

alter table PROCESSINGNODEJOBBOX
   add constraint FK_PROCESSI_RELATIONS_JOBPRIOR foreign key (JOBPRIORITY)
      references JOBPRIORITYCODE (JOBPRIORITY);

alter table PRODUCTDATASOURCE
   add constraint FK_PRODUCTD_PROD_PROD_PRODUCTD foreign key (PRODUCTID)
      references PRODUCTDESCRIPTION (PRODUCTID);

alter table PRODUCTDATASOURCE
   add constraint FK_PRODUCTD_RELATIONS_EXTERNAL foreign key (HOSTID)
      references EXTERNALDATAHOST (HOSTID);

alter table PRODUCTDATASOURCEFILECACHE
   add constraint FK_PRODUCTD_RELATIONS_PRODUCTD foreign key (PRODUCTID, HOSTID)
      references PRODUCTDATASOURCE (PRODUCTID, HOSTID);

alter table PRODUCTDESCRIPTION
   add constraint FK_PRODUCTD_PRODUCTGE_PRODUCTD foreign key (PRO_PRODUCTID)
      references PRODUCTDESCRIPTION (PRODUCTID);

alter table PRODUCTDESCRIPTION
   add constraint FK_PRODUCTD_RELATIONS_PRODUCTG foreign key (PRODUCTGROUPID)
      references PRODUCTGROUP (PRODUCTGROUPID);

alter table PRODUCTDESCRIPTION
   add constraint FK_PRODUCTD_RELATIONS_INGESTIN foreign key (INGESTINCOMINGDIRECTORYID)
      references INGESTINCOMINGDIRECTORY (INGESTINCOMINGDIRECTORYID);

alter table PRODUCTGROUPDATAPARTITION
   add constraint FK_PRODUCTG_RELATIONS_PRODUCTG foreign key (PRODUCTGROUPID)
      references PRODUCTGROUP (PRODUCTGROUPID);

alter table PRODUCTIONJOB
   add constraint FK_PRODUCTI_PRODJOBSP_PRODUCTI foreign key (PRODPARTIALJOBID)
      references PRODUCTIONJOBSPEC (PRODPARTIALJOBID);

alter table PRODUCTIONJOB
   add constraint FK_PRODUCTI_RELATIONS_PROCESSI foreign key (RESOURCEID)
      references PROCESSINGNODE (RESOURCEID);

alter table PRODUCTIONJOBLOGMESSAGES
   add constraint FK_PRODUCTI_PRODJOBLO_PRODUCTI foreign key (PRJOBID)
      references PRODUCTIONJOB (PRJOBID);

alter table PRODUCTIONJOBOUTPUTFILES
   add constraint FK_PRODUCTI_RELATIONS_PRODUCTI foreign key (PRJOBID)
      references PRODUCTIONJOB (PRJOBID);

alter table PRODUCTIONJOBSPEC
   add constraint FK_PRODUCTI_PRODJOBPR_JOBPRIOR foreign key (JOBPRIORITY)
      references JOBPRIORITYCODE (JOBPRIORITY);

alter table PRODUCTIONJOBSPEC
   add constraint FK_PRODUCTI_PRODJOBSI_JOBCLASS foreign key (JOBCLASS)
      references JOBCLASSCODE (JOBCLASS);

alter table PRODUCTIONJOBSPEC
   add constraint FK_PRODUCTI_PR_PRJOBS_PRODUCTI foreign key (PRID)
      references PRODUCTIONRULE (PRID);

alter table PRODUCTIONJOBSPEC
   add constraint FK_PRODUCTI_RELATIONS_WEATHERE foreign key (MPOID, MOVCODE, WEDID, WEID)
      references WEATHEREVENT (MPOID, MOVCODE, WEDID, WEID);

alter table PRODUCTIONRULE
   add constraint FK_PRODUCTI_PRODRULEJ_JOBCLASS foreign key (JOBCLASS)
      references JOBCLASSCODE (JOBCLASS);

alter table PRODUCTIONRULE
   add constraint FK_PRODUCTI_PRODRULEP_JOBPRIOR foreign key (JOBPRIORITY)
      references JOBPRIORITYCODE (JOBPRIORITY);

alter table PRODUCTIONRULE
   add constraint FK_PRODUCTI_PRODUCTIO_WEATHERE foreign key (MOVCODE, WEDID)
      references WEATHEREVENTDEFINITION (MOVCODE, WEDID);

alter table PRODUCTIONRULE
   add constraint FK_PRODUCTI_RELATIONS_ALGORITH foreign key (ALGORITHMID)
      references ALGORITHM (ALGORITHMID);

alter table PRODUCTIONRULE
   add constraint FK_PRODUCTI_RELATIONS_PLATFORM foreign key (PLATFORMID, PLORBITTYPEID)
      references PLATFORMORBITTYPE (PLATFORMID, PLORBITTYPEID);

alter table PRODUCTIONRULE
   add constraint FK_PRODUCTI_RELATIONS_GAZETTEE foreign key (GZID)
      references GAZETTEER (GZID);

alter table PRODUCTPLATFORM_XREF
   add constraint FK_PRODUCTP_PRODUCTPL_PLATFORM foreign key (PLATFORMID)
      references PLATFORM (PLATFORMID);

alter table PRODUCTPLATFORM_XREF
   add constraint FK_PRODUCTP_PRODUCTPL_PRODUCTD foreign key (PRODUCTID)
      references PRODUCTDESCRIPTION (PRODUCTID);

alter table PRODUCTQUALITYSUMMARY
   add constraint FK_PRODUCTQ_RELATIONS_PRODUCTD foreign key (PRODUCTID)
      references PRODUCTDESCRIPTION (PRODUCTID);

alter table PROUTPUTSPEC
   add constraint FK_PROUTPUT_RELATIONS_PRODUCTI foreign key (PRID)
      references PRODUCTIONRULE (PRID);

alter table PROUTPUTSPEC
   add constraint FK_PROUTPUT_RELATIONS_ALGOOUTP foreign key (PRODUCTID, ALGORITHMID)
      references ALGOOUTPUTPROD (PRODUCTID, ALGORITHMID);

alter table PRPARAMETER
   add constraint FK_PRPARAME_RELATIONS_PRODUCTI foreign key (PRID)
      references PRODUCTIONRULE (PRID);

alter table PRPARAMETER
   add constraint FK_PRPARAME_RELATIONS_ALGOPARA foreign key (ALGOPARAMETERID)
      references ALGOPARAMETERS (ALGOPARAMETERID);

alter table RESOURCELOGSUMMARY
   add constraint FK_RESOURCE_RM_RSL_RE_RESOURCE foreign key (RESOURCEID, RMONITORID)
      references RESOURCEMONITOR (RESOURCEID, RMONITORID);

alter table RESOURCEMEASUREMENT
   add constraint FK_RESOURCE_RMON_RMEA_RESOURCE foreign key (RESOURCEID, RMONITORID)
      references RESOURCEMONITOR (RESOURCEID, RMONITORID);

alter table RESOURCEMONITOR
   add constraint FK_RESOURCE_R_RM_REF_RESOURCE foreign key (RESOURCEID)
      references RESOURCEINSTANCE (RESOURCEID);

alter table RESOURCEMONITORALERT
   add constraint FK_RESOURCE_RM_RMA_RE_RESOURCE foreign key (RESOURCEID, RMONITORID)
      references RESOURCEMONITOR (RESOURCEID, RMONITORID);

alter table RESOURCEPLATFORM
   add constraint FK_RESOURCE_RP_R_REF_RESOURCE foreign key (RESOURCEID)
      references RESOURCEINSTANCE (RESOURCEID);

alter table RESOURCESERVER
   add constraint FK_RESOURCE_RP_RS_REF_RESOURCE foreign key (RES_RESOURCEID, RPLATFORMID)
      references RESOURCEPLATFORM (RESOURCEID, RPLATFORMID);

alter table RESOURCESERVER
   add constraint FK_RESOURCE_RP_R_REF2_RESOURCE foreign key (RESOURCEID)
      references RESOURCEINSTANCE (RESOURCEID);

alter table RESOURCESERVICE
   add constraint FK_RESOURCE_RP_R_REF3_RESOURCE foreign key (RESOURCEID)
      references RESOURCEINSTANCE (RESOURCEID);

alter table RESOURCESERVICE
   add constraint FK_RESOURCE_RSERVER_R_RESOURCE foreign key (RES_RESOURCEID, RSERVERID)
      references RESOURCESERVER (RESOURCEID, RSERVERID);

alter table SUBAPPROVALLOG
   add constraint FK_SUBAPPRO_RELATIONS_SUBSCRIP foreign key (SUBSCRIPTIONID)
      references SUBSCRIPTION (SUBSCRIPTIONID);

alter table SUBAPPROVALLOG
   add constraint FK_SUBAPPRO_RELATIONS_SUBAPPRO foreign key (SUBAPPROVALEVENTID)
      references SUBAPPROVALEVENTS (SUBAPPROVALEVENTID);

alter table SUBNOTIFICATIONTYPE
   add constraint FK_SUBNOTIF_RELATIONS_DELIVERY foreign key (DELIVERYNOTIFICATIONTYPEID)
      references DELIVERYNOTIFICATIONTYPE (DELIVERYNOTIFICATIONTYPEID);

alter table SUBNOTIFICATIONTYPE
   add constraint FK_SUBNOTIF_RELATIONS_SUBSCRIP foreign key (SUBSCRIPTIONID)
      references SUBSCRIPTION (SUBSCRIPTIONID);

alter table SUBNOTIFICATIONTYPE
   add constraint FK_SUBNOTIF_RELATIONS_EXTERNAL foreign key (HOSTID)
      references EXTERNALDATAHOST (HOSTID);

alter table SUBSCRIPTION
   add constraint FK_SUBSCRIP_RELATIONS_EXTERNAL foreign key (SUBPRIMARYHOSTID)
      references EXTERNALDATAHOST (HOSTID);

alter table SUBSCRIPTION
   add constraint FK_SUBSCRIP_RELATIONS_DISJOBCL foreign key (DICLASS)
      references DISJOBCLASSCODE (DICLASS);

alter table SUBSCRIPTION
   add constraint FK_SUBSCRIP_RELATIONS_DISJOBPR foreign key (DIPRIORITY)
      references DISJOBPRIORITYCODE (DIPRIORITY);

alter table SUBSCRIPTION
   add constraint FK_SUBSCRIP_RELATIONS_PRODUCTD foreign key (PRODUCTID)
      references PRODUCTDESCRIPTION (PRODUCTID);

alter table SUBSCRIPTION
   add constraint FK_SUBSCRIP_RELATIONS_CUSTOMER foreign key (CUSTPROFILEID)
      references CUSTOMERPROFILE (CUSTPROFILEID);

alter table SYSTEMNOTICE
   add constraint FK_SYSTEMNO_NDE_USER__NDE_USER foreign key (NDEUSERID)
      references NDE_USER (NDEUSERID);

alter table WEATHEREVENT
   add constraint FK_WEATHERE_RELATIONS_METEOPOI foreign key (MPOID)
      references METEOPOINTOBS (MPOID);

alter table WEATHEREVENT
   add constraint FK_WEATHERE_RELATIONS_WEATHERE foreign key (MOVCODE, WEDID)
      references WEATHEREVENTDEFINITION (MOVCODE, WEDID);

alter table WEATHEREVENTDEFINITION
   add constraint FK_WEATHERE_WED_MOV_B_METEOOBS foreign key (MOVCODE)
      references METEOOBSVARIABLES (MOVCODE);

