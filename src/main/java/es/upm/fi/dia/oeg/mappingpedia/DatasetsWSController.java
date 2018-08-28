package es.upm.fi.dia.oeg.mappingpedia;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.annotation.MultipartConfig;

import es.upm.fi.dia.oeg.mappingpedia.controller.DatasetController;
import es.upm.fi.dia.oeg.mappingpedia.controller.DistributionController;
import es.upm.fi.dia.oeg.mappingpedia.model.*;
import es.upm.fi.dia.oeg.mappingpedia.model.result.*;
//import es.upm.fi.dia.oeg.mappingpedia.utility.*;
import es.upm.fi.dia.oeg.mappingpedia.utility.MpcCkanUtility;
import es.upm.fi.dia.oeg.mappingpedia.utility.MappingPediaUtility;
import es.upm.fi.dia.oeg.mappingpedia.utility.MpcUtility;
import org.apache.commons.io.FileUtils;
//import org.apache.jena.ontology.OntModel;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RestController
//@RequestMapping(value = "/mappingpedia")
@MultipartConfig(fileSizeThreshold = 20971520)
public class DatasetsWSController {
    static Logger logger = LoggerFactory.getLogger(DatasetsWSController.class);

    private static final String template = "Hello, %s!";
    private final AtomicLong counter = new AtomicLong();


    //private OntModel ontModel = MappingPediaEngine.ontologyModel();

    /*
    private GitHubUtility githubClient = MappingPediaEngine.githubClient();
    private CKANUtility ckanClient = MappingPediaEngine.ckanClient();
    private JenaClient jenaClient = MappingPediaEngine.jenaClient();
    private VirtuosoClient virtuosoClient = MappingPediaEngine.virtuosoClient();
*/
    private DatasetController datasetController = DatasetController.apply();
    private DistributionController distributionController = DistributionController.apply();
    private MpcCkanUtility ckanClient = datasetController.ckanClient();

    @RequestMapping(value="/greeting", method= RequestMethod.GET)
    public GreetingJava getGreeting(@RequestParam(value="name", defaultValue="World") String name) {
        logger.info("/greeting(GET) ...");
        return new GreetingJava(counter.incrementAndGet(),
                String.format(template, name));
    }

    @RequestMapping(value="/", method= RequestMethod.GET, produces={"application/ld+json"})
    public Inbox get() {
        logger.info("GET / ...");
        return new Inbox();
    }

    @RequestMapping(value="/", method= RequestMethod.HEAD, produces={"application/ld+json"})
    public ResponseEntity head() {
        logger.info("HEAD / ...");
        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.LINK, "<http://mappingpedia-engine.linkeddata.es/inbox>; rel=\"http://www.w3.org/ns/ldp#inbox\"");

        return new ResponseEntity(headers, HttpStatus.CREATED);
    }

    @RequestMapping(value="/inbox", method= RequestMethod.POST)
    public GeneralResult postInbox(
            //@RequestParam(value="notification", required = false) Object notification)
            @RequestBody Object notification
    )
    {
        logger.info("POST /inbox ...");
        logger.info("notification = " + notification);
        return new GeneralResult(HttpStatus.OK.getReasonPhrase(), HttpStatus.OK.value());
    }

    @RequestMapping(value="/inbox", method= RequestMethod.PUT)
    public GeneralResult putInbox(
            //@RequestParam(value="notification", defaultValue="") String notification
            @RequestBody Object notification
    )
    {
        logger.info("PUT /inbox ...");
        logger.info("notification = " + notification);
        return new GeneralResult(HttpStatus.OK.getReasonPhrase(), HttpStatus.OK.value());
    }



    /*
    @RequestMapping(value="/greeting/{name}", method= RequestMethod.PUT)
    public GreetingJava putGreeting(@PathVariable("name") String name) {
        logger.info("/greeting(PUT) ...");
        return new GreetingJava(counter.incrementAndGet(),
                String.format(template, name));
    }
    */

    @RequestMapping(value="/distributions/{organization_id}/{dataset_id}/{distribution_id}/modified"
            , method= RequestMethod.PUT)
    public GeneralResult putDistributionsModifiedDate(
            @PathVariable("organization_id") String organizationId
            , @PathVariable("dataset_id") String datasetId
            , @PathVariable("distribution_id") String distributionId
    )
    {
        logger.info("[PUT] /distributions/{organization_id}/{dataset_id}");
        logger.info("organization_id = " + organizationId);
        logger.info("dataset_id = " + datasetId);
        logger.info("distribution_id = " + distributionId);

        Distribution distribution = new UnannotatedDistribution(
                organizationId, datasetId, distributionId);

        this.distributionController.addModifiedDate(distribution);

        return new GeneralResult(HttpStatus.OK.getReasonPhrase(), HttpStatus.OK.value());
    }

    @RequestMapping(value="/datasets/{organization_id}/{dataset_id}/modified"
            , method= RequestMethod.PUT)
    public GeneralResult putDatasetModifiedDate(
            @PathVariable("organization_id") String organizationId
            , @PathVariable("dataset_id") String datasetId
    )
    {
        logger.info("[PUT] /datasets/{organization_id}/{dataset_id}");
        logger.info("organization_id = " + organizationId);
        logger.info("dataset_id = " + datasetId);

        Dataset dataset = new Dataset(organizationId, datasetId);

        this.datasetController.addModifiedDate(dataset);

        return new GeneralResult(HttpStatus.OK.getReasonPhrase(), HttpStatus.OK.value());
    }


    @RequestMapping(value="/github_repo_url", method= RequestMethod.GET)
    public String getGitHubRepoURL() {
        logger.info("GET /github_repo_url ...");
        return MappingPediaEngine.mappingpediaProperties().githubRepository();
    }

    @RequestMapping(value="/ckan_datasets", method= RequestMethod.GET)
    public ListResult getCKANDatasets(@RequestParam(value="catalogUrl", required = false) String catalogUrl) {
        if(catalogUrl == null) {
            catalogUrl = MappingPediaEngine.mappingpediaProperties().ckanURL();
        }
        logger.info("GET /ckanDatasetList ...");
        return MpcCkanUtility.getDatasetList(catalogUrl);
    }

    @RequestMapping(value="/virtuoso_enabled", method= RequestMethod.GET)
    public String getVirtuosoEnabled() {
        logger.info("GET /virtuosoEnabled ...");
        return MappingPediaEngine.mappingpediaProperties().virtuosoEnabled() + "";
    }

    @RequestMapping(value="/mappingpedia_graph", method= RequestMethod.GET)
    public String getMappingpediaGraph() {
        logger.info("/getMappingPediaGraph(GET) ...");
        return MappingPediaEngine.mappingpediaProperties().graphName();
    }

    @RequestMapping(value="/ckan_api_action_organization_create", method= RequestMethod.GET)
    public String getCKANAPIActionOrganizationCreate() {
        logger.info("GET /ckanActionOrganizationCreate ...");
        return MappingPediaEngine.mappingpediaProperties().ckanActionOrganizationCreate();
    }

    @RequestMapping(value="/ckan_api_action_package_create", method= RequestMethod.GET)
    public String getCKANAPIActionPpackageCreate() {
        logger.info("GET /ckanActionPackageCreate ...");
        return MappingPediaEngine.mappingpediaProperties().ckanActionPackageCreate();
    }

    @RequestMapping(value="/ckan_api_action_resource_create", method= RequestMethod.GET)
    public String getCKANAPIActionResourceCreate() {
        logger.info("GET /getCKANActionResourceCreate ...");
        return MappingPediaEngine.mappingpediaProperties().ckanActionResourceCreate();
    }

    @RequestMapping(value="/ckan_resource_id", method= RequestMethod.GET)
    public String getCKANResourceIdByResourceUrl(
            @RequestParam(value="package_id", required = true) String packageId
            , @RequestParam(value="resource_url", required = true) String resourceUrl
    ) {
        logger.info("GET /ckan_resource_id ...");
        logger.info("package_id = " + packageId);
        logger.info("resource_url = " + resourceUrl);

        String result = this.ckanClient.getResourceIdByResourceUrl(packageId, resourceUrl);

        return result;

    }

    @RequestMapping(value="/ckan_annotated_resources_ids", method= RequestMethod.GET)
    public ListResult<String> getCKANAnnotatedResourcesIds(
            @RequestParam(value="package_id", required = true) String packageId
    ) {
        logger.info("GET /ckan_annotated_resources_ids ...");
        logger.info("this.ckanClient = " + this.ckanClient);

        ListResult<String> result = this.ckanClient.getAnnotatedResourcesIdsAsListResult(packageId);
        return result;
    }

    @RequestMapping(value="/ckan_resource_url", method= RequestMethod.GET)
    public ListResult<String> getCKANResourceUrl(
            @RequestParam(value="resource_id", required = true) String resourceId
    ) {
        logger.info("GET /ckan_resource_url ...");
        ListResult<String> result = this.ckanClient.getResourcesUrlsAsListResult(resourceId);
        return result;
    }

    @RequestMapping(value="/ckanResource", method= RequestMethod.POST)
    public Integer postCKANResource(
            @RequestParam(value="filePath", required = true) String filePath
            , @RequestParam(value="packageId", required = true) String packageId
    ) {
        logger.info("POST /ckanResource...");
        String ckanURL = MappingPediaEngine.mappingpediaProperties().ckanURL();
        String ckanKey = MappingPediaEngine.mappingpediaProperties().ckanKey();

        MpcCkanUtility ckanClient = new MpcCkanUtility(ckanURL, ckanKey);
        File file = new File(filePath);
        try {
            if(!file.exists()) {
                String fileName = file.getName();
                file = new File(fileName);
                FileUtils.copyURLToFile(new URL(filePath), file);
            }
        } catch(Exception e) {
            e.printStackTrace();
        }

        //return ckanUtility.createResource(file.getPath(), packageId);
        return null;
    }

    @RequestMapping(value="/dataset_language/{organizationId}", method= RequestMethod.POST)
    public Integer postDatasetLanguage(
            @PathVariable("organizationId") String organizationId
            , @RequestParam(value="dataset_language", required = true) String datasetLanguage
    ) {
        logger.info("POST /dataset_language ...");
        String ckanURL = MappingPediaEngine.mappingpediaProperties().ckanURL();
        String ckanKey = MappingPediaEngine.mappingpediaProperties().ckanKey();

        MpcCkanUtility ckanClient = new MpcCkanUtility(ckanURL, ckanKey);
        return ckanClient.updateDatasetLanguage(organizationId, datasetLanguage);
    }

    @RequestMapping(value="/triples_maps", method= RequestMethod.GET)
    public ListResult getTriplesMaps() {
        logger.info("/triplesMaps ...");
        ListResult listResult = MappingPediaEngine.getAllTriplesMaps();
        //logger.info("listResult = " + listResult);

        return listResult;
    }


    @RequestMapping(value="/datasets", method= RequestMethod.GET)
    public ListResult getDatasets(
            @RequestParam(value="ckan_package_id", required = false) String ckanPackageId
            , @RequestParam(value="ckan_package_name", required = false) String ckanPackageName
    ) {
        logger.info("/datasets ...");
        logger.info("ckan_package_id = " + ckanPackageId);
        logger.info("ckan_package_name = " + ckanPackageName);

        ListResult listResult;

        if(ckanPackageId != null && ckanPackageName == null) {
            listResult = this.datasetController.findByCKANPackageId(ckanPackageId);
        } else if(ckanPackageId == null && ckanPackageName != null) {
            listResult = this.datasetController.findByCKANPackageName(ckanPackageName);
        } else {
            listResult = this.datasetController.findAll();
        }
        //logger.info("datasets result = " + listResult);

        return listResult;
    }

    @RequestMapping(value="/dataset", method= RequestMethod.GET)
    public ListResult getDataset(
            @RequestParam(value="dataset_id", required = false) String datasetId
    ) {
        logger.info("/dataset ...");
        logger.info("dataset_id = " + datasetId);

        ListResult listResult = this.datasetController.findById(datasetId);

        return listResult;
    }

    @RequestMapping(value="/distributions", method= RequestMethod.GET)
    public ListResult getDistributions(
            @RequestParam(value="ckan_resource_id", required = false) String ckanResourceId
    ) {
        logger.info("/distributions ...");
        logger.info("ckan_resource_id = " + ckanResourceId);

        ListResult listResult = this.distributionController.findByCKANResourceId(
                ckanResourceId);

        logger.info("/distributions listResult = " + listResult);

        return listResult;
    }


    /*    //TODO REFACTOR THIS; MERGE /executions with /executions2
    //@RequestMapping(value="/executions1/{organizationId}/{datasetId}/{mappingFilename:.+}"
//            , method= RequestMethod.POST)
    @RequestMapping(value="/executions1/{organizationId}/{datasetId}/{mappingDocumentId}"
            , method= RequestMethod.POST)
    public ExecuteMappingResult postExecutions1(
            @PathVariable("organization_id") String organizationId

            , @PathVariable("dataset_id") String datasetId
            , @RequestParam(value="distribution_access_url", required = false) String distributionAccessURL
            , @RequestParam(value="distribution_download_url", required = false) String distributionDownloadURL
            , @RequestParam(value="distribution_mediatype", required = false, defaultValue="text/csv") String distributionMediaType
            , @RequestParam(value="field_separator", required = false) String fieldSeparator

            , @RequestParam(value="mapping_document_id", required = false) String mappingDocumentId
            , @RequestParam(value="mapping_document_download_url", required = false) String mappingDocumentDownloadURL
            , @RequestParam(value="mapping_language", required = false) String pMappingLanguage

            , @RequestParam(value="query_file", required = false) String queryFile
            , @RequestParam(value="output_filename", required = false) String outputFilename

            , @RequestParam(value="db_username", required = false) String dbUserName
            , @RequestParam(value="db_password", required = false) String dbPassword
            , @RequestParam(value="db_name", required = false) String dbName
            , @RequestParam(value="jdbc_url", required = false) String jdbc_url
            , @RequestParam(value="database_driver", required = false) String databaseDriver
            , @RequestParam(value="database_type", required = false) String databaseType

            , @RequestParam(value="use_cache", required = false) String pUseCache
            //, @PathVariable("mappingFilename") String mappingFilename
    )
    {
        logger.info("POST /executions1/{organizationId}/{datasetId}/{mappingDocumentId}");
        logger.info("mapping_document_id = " + mappingDocumentId);

        Agent organization = new Agent(organizationId);

        Dataset dataset = new Dataset(organization, datasetId);
        Distribution distribution = new Distribution(dataset);
        if(distributionAccessURL != null) {
            distribution.dcatAccessURL_$eq(distributionAccessURL);
        }
        if(distributionDownloadURL != null) {
            distribution.dcatDownloadURL_$eq(distributionDownloadURL);
        } else {
            distribution.dcatDownloadURL_$eq(this.githubClient.getDownloadURL(distributionAccessURL));
        }
        if(fieldSeparator != null) {
            distribution.cvsFieldSeparator_$eq(fieldSeparator);
        }
        distribution.dcatMediaType_$eq(distributionMediaType);
        dataset.addDistribution(distribution);


        MappingDocument md = new MappingDocument();
        if(mappingDocumentDownloadURL != null) {
            md.setDownloadURL(mappingDocumentDownloadURL);
        } else {
            if(mappingDocumentId != null) {
                MappingDocument foundMappingDocument = this.mappingDocumentController.findMappingDocumentsByMappingDocumentId(mappingDocumentId);
                md.setDownloadURL(foundMappingDocument.getDownloadURL());
            } else {
                //I don't know that to do here, Ahmad will handle
            }
        }

        if(pMappingLanguage != null) {
            md.mappingLanguage_$eq(pMappingLanguage);
        } else {
            String mappingLanguage = MappingDocumentController.detectMappingLanguage(mappingDocumentDownloadURL);
            logger.info("mappingLanguage = " + mappingLanguage);
            md.mappingLanguage_$eq(mappingLanguage);
        }


        JDBCConnection jdbcConnection = new JDBCConnection(dbUserName, dbPassword
                , dbName, jdbc_url
                , databaseDriver, databaseType);


        Boolean useCache = MappingPediaUtility.stringToBoolean(pUseCache);
        try {
            //IN THIS PARTICULAR CASE WE HAVE TO STORE THE EXECUTION RESULT ON CKAN
            return mappingExecutionController.executeMapping(md, dataset, queryFile, outputFilename
                    , true, true, true, jdbcConnection
                    , useCache

            );
        } catch (Exception e) {
            e.printStackTrace();
            String errorMessage = "Error occured: " + e.getMessage();
            logger.error("mapping execution failed: " + errorMessage);
            ExecuteMappingResult executeMappingResult = new ExecuteMappingResult(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, "Internal Error"
                    , null, null
                    , null
                    , null, null
                    , null
                    , null
                    , null, null
            );
            return executeMappingResult;
        }
    }*/


    @RequestMapping(value = "/datasets/{organization_id}", method= RequestMethod.POST)
    public AddDatasetResult postDatasets2(
            @PathVariable("organization_id") String organizationId

            //FIELDS RELATED TO DATASET/PACKAGE
            , @RequestParam(value="dataset_id", required = false) String datasetID
            , @RequestParam(value="dataset_title", required = false) String pDatasetTitle1
            , @RequestParam(value="datasetTitle", required = false) String pDatasetTitle2
            , @RequestParam(value="dataset_keywords", required = false) String pDatasetKeywords1
            , @RequestParam(value="datasetKeywords", required = false) String pDatasetKeywords2
            , @RequestParam(value="dataset_category", required = false) String datasetCategory
            , @RequestParam(value="dataset_language", required = false) String pDatasetLanguage1
            , @RequestParam(value="datasetLanguage", required = false) String pDatasetLanguage2
            , @RequestParam(value="dataset_description", required = false) String pDatasetDescription1
            , @RequestParam(value="datasetDescription", required = false) String pDatasetDescription2
            , @RequestParam(value="source", required = false, defaultValue = "") String ckanSource
            , @RequestParam(value="version", required = false, defaultValue = "") String ckanVersion
            , @RequestParam(value="author_name", required = false, defaultValue = "") String ckanAuthorName
            , @RequestParam(value="author_email", required = false, defaultValue = "") String ckanAuthorEmail
            , @RequestParam(value="maintainer_name", required = false, defaultValue = "") String ckanMaintainerName
            , @RequestParam(value="maintainer_email", required = false, defaultValue = "") String ckanMaintainerEmail
            , @RequestParam(value="temporal", required = false, defaultValue = "") String ckanTemporal
            , @RequestParam(value="spatial", required = false, defaultValue = "") String ckanSpatial
            , @RequestParam(value="accrual_periodicity", required = false, defaultValue = "") String ckanAccrualPeriodicity
            , @RequestParam(value="access_right", required = false, defaultValue = "") String accessRight
            , @RequestParam(value="provenance", required = false, defaultValue = "") String provenance
            , @RequestParam(value="dataset_license", required = false) String datasetLicense

            //FIELDS RELATED TO DISTRIBUTION/RESOURCE
            , @RequestParam(value="distribution_file", required = false) MultipartFile pDistributionFile1
            , @RequestParam(value="distributionFile", required = false) MultipartFile pDistributionFile2
            , @RequestParam(value="distribution_access_url", required = false) String distributionAccessURL
            , @RequestParam(value="distribution_download_url", required = false) String distributionDownloadURL
            , @RequestParam(value="distributionMediaType", required = false, defaultValue="text/csv") String distributionMediaType
            , @RequestParam(value="distributionDescription", required = false) String distributionDescription
            , @RequestParam(value="distribution_encoding", required = false, defaultValue="UTF-8") String distributionEncoding

            //FIELDS RELATED TO PROV
            , @RequestParam(value="was_attributed_to", required = false) String provWasAttributedTo
            , @RequestParam(value="was_generated_by", required = false) String provWasGeneratedBy
            , @RequestParam(value="was_derived_from", required = false) String provWasDerivedFrom
            , @RequestParam(value="specialization_of", required = false) String provSpecializationOf
            , @RequestParam(value="had_primary_source", required = false) String provHadPrimarySource
            , @RequestParam(value="was_revision_of", required = false) String provWasRevisionOf
            , @RequestParam(value="was_influenced_by", required = false) String provWasInfluencedBy


            //OTHER FIELDS
            , @RequestParam(value="manifestFile", required = false) MultipartFile manifestFileRef
            , @RequestParam(value="generateManifestFile", required = false, defaultValue="true") String pGenerateManifestFile
            , @RequestParam(value="ckan_package_id", required = false) String ckanPackageId
            , @RequestParam(value="store_to_ckan", required = false, defaultValue = "true") String pStoreToCKAN
            , @RequestParam(value="ckan_organization_id", required = false) String ckanOrganizationId
            , @RequestParam(value="ckan_organization_name", required = false) String ckanOrganizationName
    )
    {
        logger.info("[POST] /datasets/{organization_id}");
        logger.info("organization_id = " + organizationId);
        logger.debug("dataset_id = " + datasetID);
        logger.info("distribution_download_url = " + distributionDownloadURL);
        logger.info("distribution_file = " + pDistributionFile1);
        logger.info("datasetLicense = " + datasetLicense);

        logger.info("was_attributed_to = " + provWasAttributedTo);
        logger.info("was_generated_by = " + provWasGeneratedBy);
        logger.info("was_derived_from = " + provWasDerivedFrom);
        logger.info("specialization_of = " + provSpecializationOf);
        logger.info("had_primary_source = " + provHadPrimarySource);
        logger.info("was_revision_of = " + provWasRevisionOf);
        logger.info("was_influenced_by = " + provWasInfluencedBy);

        try {
            boolean generateManifestFile = MappingPediaUtility.stringToBoolean(pGenerateManifestFile);

            Dataset dataset = Dataset.apply(organizationId, datasetID);
            dataset.setTitle(pDatasetTitle1, pDatasetTitle2);
            dataset.setDescription(pDatasetDescription1, pDatasetDescription2);
            dataset.setKeywords(pDatasetKeywords1, pDatasetKeywords2);
            dataset.setLanguage(pDatasetLanguage1, pDatasetLanguage2);
            dataset.mvpCategory_$eq(datasetCategory);
            dataset.dctSource_$eq(ckanSource);
            dataset.ckanVersion_$eq(ckanVersion);
            dataset.setAuthor(ckanAuthorName, ckanAuthorEmail);
            dataset.setMaintainer(ckanMaintainerName, ckanMaintainerEmail);
            dataset.ckanTemporal_$eq(ckanTemporal);
            dataset.ckanSpatial_$eq(ckanSpatial);
            dataset.ckanAccrualPeriodicity_$eq(ckanAccrualPeriodicity);
            dataset.dctAccessRight_$eq(accessRight);
            dataset.dctProvenance_$eq(provenance);
            dataset.ckanPackageLicense_$eq(datasetLicense);
            dataset.ckanOrganizationId_$eq(ckanOrganizationId);
            dataset.ckanOrganizationName_$eq(ckanOrganizationName);

            dataset.provWasAttributedTo_$eq(provWasAttributedTo);
            dataset.provWasGeneratedBy_$eq(provWasGeneratedBy);
            dataset.provWasDerivedFrom_$eq(provWasDerivedFrom);
            dataset.provSpecializationOf_$eq(provSpecializationOf);
            dataset.provHadPrimarySource_$eq(provHadPrimarySource);
            dataset.provWasRevisionOf_$eq(provWasRevisionOf);
            dataset.provWasInfluencedBy_$eq(provWasInfluencedBy);


            if(distributionDownloadURL != null || pDistributionFile1 != null
                    || pDistributionFile2 != null) {
                Distribution distribution = new UnannotatedDistribution(dataset);
                distribution.setDistributionFile(pDistributionFile1, pDistributionFile2);
                distribution.dcatDownloadURL_$eq(distributionDownloadURL);
                distribution.setDescription(distributionDescription);
                distribution.dcatMediaType_$eq(distributionMediaType);
                distribution.encoding_$eq(distributionEncoding);
                dataset.addDistribution(distribution);
            }

            boolean storeToCKAN = MappingPediaUtility.stringToBoolean(pStoreToCKAN);
            File manifestFile = MpcUtility.multipartFileToFile(manifestFileRef, dataset.dctIdentifier());

            return this.datasetController.add(dataset, manifestFile
                    , generateManifestFile, storeToCKAN);
        } catch(Exception e) {
            e.printStackTrace();
            return new AddDatasetResult(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage()
                    , null
                    , null, null
                    , null, null
            );
        }

    }

    //LEGACY ENDPOINT, use /distributions/{organizationID}/{datasetID} instead
    @RequestMapping(value = "/datasets/{organization_id}/{dataset_id}", method= RequestMethod.POST)
    public AddDistributionResult postDatasets1(
            @PathVariable("organization_id") String organizationId
            , @PathVariable("dataset_id") String datasetId
            , @RequestParam(value="datasetFile", required = false) MultipartFile distributionFileRef
            , @RequestParam(value="datasetTitle", required = false) String distributionTitle
            , @RequestParam(value="datasetKeywords", required = false) String datasetKeywords
            , @RequestParam(value="datasetPublisher", required = false) String datasetPublisher
            , @RequestParam(value="datasetLanguage", required = false) String datasetLanguage
            , @RequestParam(value="manifestFile", required = false) MultipartFile manifestFileRef
            , @RequestParam(value="generateManifestFile", required = false, defaultValue="true") String pGenerateManifestFile

            , @RequestParam(value="distribution_access_url", required = false) String distributionAccessURL
            , @RequestParam(value="distribution_download_url", required = false) String distributionDownloadURL
            , @RequestParam(value="distributionMediaType", required = false, defaultValue="text/csv") String distributionMediaType


            , @RequestParam(value="datasetDescription", required = false) String distributionDescription
            , @RequestParam(value="store_to_ckan", defaultValue = "true") String pStoreToCKAN
    )
    {
        logger.info("[POST] /datasets/{organization_id}/{dataset_id}");
        logger.info("organization_id = " + organizationId);
        logger.info("dataset_id = " + datasetId);
        boolean generateManifestFile = MappingPediaUtility.stringToBoolean(pGenerateManifestFile);

        Agent organization = new Agent(organizationId);

        Dataset dataset = new Dataset(organization, datasetId);
        dataset.dcatKeyword_$eq(datasetKeywords);
        dataset.dctLanguage_$eq(datasetLanguage);

        Distribution distribution = new UnannotatedDistribution(dataset);
        if(distributionTitle == null) {
            distribution.dctTitle_$eq(distribution.dctIdentifier());
        } else {
            distribution.dctTitle_$eq(distributionTitle);
        }
        if(distributionDescription == null) {
            distribution.dctDescription_$eq(distribution.dctIdentifier());
        } else {
            distribution.dctDescription_$eq(distributionDescription);
        }
        if(distributionAccessURL == null) {
            distribution.dcatAccessURL_$eq(distributionDownloadURL);
        } else {
            distribution.dcatAccessURL_$eq(distributionAccessURL);
        }
        distribution.dcatDownloadURL_$eq(distributionDownloadURL);
        distribution.dcatMediaType_$eq(distributionMediaType);
        if(distributionFileRef != null) {
            distribution.distributionFile_$eq(MpcUtility.multipartFileToFile(
                    distributionFileRef , dataset.dctIdentifier()));
        }
        dataset.addDistribution(distribution);

        boolean storeToCKAN = true;
        if("false".equalsIgnoreCase("pStoreToCKAN")
                || "true".equalsIgnoreCase(pStoreToCKAN)) {
            storeToCKAN = false;
        }

        File manifestFile = MpcUtility.multipartFileToFile(manifestFileRef, dataset.dctIdentifier());

        return this.distributionController.addDistribution(distribution, manifestFile
                , generateManifestFile, storeToCKAN);
    }

    @RequestMapping(value = "/distributions/{organization_id}", method= RequestMethod.POST)
    public AddDistributionResult postDistributions2(
            @PathVariable("organization_id") String organizationID
            , @RequestParam(value="dataset_id", required = false) String pDatasetId
            , @RequestParam(value="ckan_package_id", required = false) String ckanPackageId
            , @RequestParam(value="ckan_package_name", required = false) String ckanPackageName
            , @RequestParam(value="distribution_download_url", required = false) String distributionDownloadURL
            , @RequestParam(value="manifestFile", required = false) MultipartFile manifestFileRef
            , @RequestParam(value="generateManifestFile", required = false, defaultValue="true") String pGenerateManifestFile
            //, @RequestParam(value="datasetFile", required = false) MultipartFile datasetMultipartFile
            , @RequestParam(value="distribution_file", required = false) MultipartFile distributionMultipartFile
            , @RequestParam(value="distribution_title", required = false) String distributionTitle
            , @RequestParam(value="distributionAccessURL", required = false) String distributionAccessURL
            , @RequestParam(value="distributionMediaType", required = false, defaultValue="text/csv") String distributionMediaType
            , @RequestParam(value="distributionDescription", required = false) String distributionDescription
            , @RequestParam(value="distribution_encoding", required = false, defaultValue="UTF-8") String distributionEncoding
            , @RequestParam(value="store_to_ckan", defaultValue = "true") String pStoreToCKAN
            , @RequestParam(value="distribution_language", required = false) String distributionLanguage
            , @RequestParam(value="distribution_license", required = false) String distributionLicense
            , @RequestParam(value="distribution_rights", required = false) String distributionRights
    )
    {
        logger.info("[POST] /distributions/{organization_id}");
        logger.info("organization_id = " + organizationID);
        logger.info("dataset_id = " + pDatasetId);
        logger.info("ckan_package_id = " + ckanPackageId);
        logger.info("ckan_package_name = " + ckanPackageName);
        logger.info("distribution_download_url = " + distributionDownloadURL);
        logger.info("distribution_file = " + distributionMultipartFile);
        boolean generateManifestFile = MappingPediaUtility.stringToBoolean(pGenerateManifestFile);

        try {
            Dataset dataset = this.datasetController.findOrCreate(
                    organizationID, pDatasetId, ckanPackageId, ckanPackageName);

            Distribution distribution = new UnannotatedDistribution(dataset);
            distribution.setTitle(distributionTitle);
            distribution.setDescription(distributionDescription);
            distribution.dcatDownloadURL_$eq(distributionDownloadURL);
            distribution.setAccessURL(distributionAccessURL, distributionDownloadURL);
            distribution.dcatMediaType_$eq(distributionMediaType);
            if(distributionMultipartFile != null) {
                distribution.distributionFile_$eq(MpcUtility.multipartFileToFile(
                        distributionMultipartFile , dataset.dctIdentifier()));
            }
            distribution.encoding_$eq(distributionEncoding);
            distribution.setLanguage(distributionLanguage);
            distribution.dctLicense_$eq(distributionLicense);
            distribution.dctRights_$eq(distributionRights);
            dataset.addDistribution(distribution);

            boolean storeToCKAN = true;
            if("false".equalsIgnoreCase("pStoreToCKAN")
                    || "no".equalsIgnoreCase(pStoreToCKAN)) {
                storeToCKAN = false;
            }

            File manifestFile = MpcUtility.multipartFileToFile(manifestFileRef, dataset.dctIdentifier());
            return this.distributionController.addDistribution(distribution, manifestFile
                    , generateManifestFile, storeToCKAN);

        } catch (Exception e) {
            e.printStackTrace();
            return new AddDistributionResult(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage()
                    , null
                    , null, null
                    , null, null
                    , null
                    , null
            );
        }
    }

    @RequestMapping(value = "/distributions/{organization_id}/{dataset_id}", method= RequestMethod.POST)
    public AddDistributionResult postDistributions1(
            @PathVariable("organization_id") String organizationID
            , @PathVariable("dataset_id") String datasetID
            , @RequestParam(value="distribution_download_url", required = false) String distributionDownloadURL
            , @RequestParam(value="manifestFile", required = false) MultipartFile manifestFileRef
            , @RequestParam(value="generateManifestFile", required = false, defaultValue="true") String pGenerateManifestFile
            //, @RequestParam(value="datasetFile", required = false) MultipartFile datasetMultipartFile
            , @RequestParam(value="distribution_file", required = false) MultipartFile distributionMultipartFile
            , @RequestParam(value="distribution_title", required = false) String distributionTitle
            , @RequestParam(value="distributionAccessURL", required = false) String distributionAccessURL
            , @RequestParam(value="distributionMediaType", required = false, defaultValue="text/csv") String distributionMediaType
            , @RequestParam(value="distributionDescription", required = false) String distributionDescription
            , @RequestParam(value="distribution_encoding", required = false, defaultValue="UTF-8") String distributionEncoding
            , @RequestParam(value="store_to_ckan", defaultValue = "true") String pStoreToCKAN
            , @RequestParam(value="distribution_language", required = false) String distributionLanguage
            , @RequestParam(value="distribution_license", required = false) String distributionLicense
            , @RequestParam(value="distribution_rights", required = false) String distributionRights
    )
    {
        logger.info("[POST] /distributions/{organization_id}/{dataset_id}");
        logger.info("organization_id = " + organizationID);
        logger.info("dataset_id = " + datasetID);
        logger.info("distribution_download_url = " + distributionDownloadURL);
        logger.info("distribution_file = " + distributionMultipartFile);
        boolean generateManifestFile = MappingPediaUtility.stringToBoolean(pGenerateManifestFile);

        Agent organization = new Agent(organizationID);

        Dataset dataset = new Dataset(organization, datasetID);

        Distribution distribution = new UnannotatedDistribution(dataset);
        distribution.setTitle(distributionTitle);
        distribution.setDescription(distributionDescription);
        distribution.dcatDownloadURL_$eq(distributionDownloadURL);
        distribution.setAccessURL(distributionAccessURL, distributionDownloadURL);
        distribution.dcatMediaType_$eq(distributionMediaType);
        if(distributionMultipartFile != null) {
            distribution.distributionFile_$eq(MpcUtility.multipartFileToFile(
                    distributionMultipartFile , dataset.dctIdentifier()));
        }
        distribution.encoding_$eq(distributionEncoding);
        distribution.setLanguage(distributionLanguage);
        distribution.dctLicense_$eq(distributionLicense);
        distribution.dctRights_$eq(distributionRights);
        dataset.addDistribution(distribution);

        boolean storeToCKAN = true;
        if("false".equalsIgnoreCase("pStoreToCKAN")
                || "no".equalsIgnoreCase(pStoreToCKAN)) {
            storeToCKAN = false;
        }

        File manifestFile = MpcUtility.multipartFileToFile(manifestFileRef, dataset.dctIdentifier());
        return this.distributionController.addDistribution(distribution, manifestFile
                , generateManifestFile, storeToCKAN);
    }

    @RequestMapping(value = "/queries/{mappingpediaUsername}/{datasetID}", method= RequestMethod.POST)
    public GeneralResult postQueries(
            @RequestParam("queryFile") MultipartFile queryFileRef
            , @PathVariable("mappingpediaUsername") String mappingpediaUsername
            , @PathVariable("datasetID") String datasetID
    )
    {
        logger.info("[POST] /queries/{mappingpediaUsername}/{datasetID}");
        return MappingPediaEngine.addQueryFile(queryFileRef, mappingpediaUsername, datasetID);
    }


    @RequestMapping(value = "/rdf_file", method= RequestMethod.POST)
    public GeneralResult postRDFFile(
            @RequestParam("rdfFile") MultipartFile fileRef
            , @RequestParam(value="graphURI") String graphURI)
    {
        logger.info("/storeRDFFile...");
        return MappingPediaEngine.storeRDFFile(fileRef, graphURI);
    }

    @RequestMapping(value="/ogd/utility/subclasses", method= RequestMethod.GET)
    public ListResult getSubclassesDetails(
            @RequestParam(value="aClass") String aClass
    ) {
        logger.info("GET /ogd/utility/subclasses ...");
        logger.info("aClass = " + aClass);
        ListResult result = MappingPediaEngine.getSchemaOrgSubclassesDetail(aClass) ;
        //logger.info("result = " + result);
        return result;
    }

    @RequestMapping(value="/ogd/utility/subclassesSummary", method= RequestMethod.GET)
    public ListResult getSubclassesSummary(
            @RequestParam(value="aClass") String aClass
    ) {
        logger.info("GET /ogd/utility/subclassesSummary ...");
        logger.info("aClass = " + aClass);
        ListResult result = MappingPediaEngine.getSubclassesSummary(aClass) ;
        //logger.info("result = " + result);
        return result;
    }

}