package com.iceleads.core.ejb.session.impl;

import com.commons.util.DateFormatUtil;
import com.commons.util.MessageDigestUtil;
import com.commons.util.TimeUtil;
import com.iceleads.commons.mail.constants.NotificationKeyEnum;
import com.iceleads.constants.enm.RefActivityTypeEnum;
import com.iceleads.constants.web.RestrictValueEnum;
import com.iceleads.core.converter.IndexConverter;
import com.iceleads.core.ejb.session.remote.IIndexManagerRemote;
import com.iceleads.dao.factory.AbstractIceleadsDAOFactory;
import com.iceleads.dao.factory.AbstractIceleadsSolrDAOFactory;
import com.iceleads.dao.factory.AbstractIceleadsStagingDAOFactory;
import com.iceleads.dao.factory.AbstractIceleadsStatDAOFactory;
import com.iceleads.dao.factory.DAOFactory;
import com.iceleads.dao.impl.CoreProductRepositoryDAO;
import com.iceleads.dao.interfaces.*;
import com.iceleads.dao.solr.interfaces.ISolrIndexProductDAO;
import com.iceleads.dao.solr.staging.interfaces.ISolrStagingProductDAO;
import com.iceleads.dao.staging.interfaces.IMerchantFeedMapDAO;
import com.iceleads.dao.stat.interfaces.IStatIndexStagingMapDAO;
import com.iceleads.dao.util.DAOSessionUtil;
import com.iceleads.data.*;
import com.iceleads.data.staging.MerchantFeedMapDomain;
import com.iceleads.data.stat.StatIndexStagingMapDomain;
import com.iceleads.dto.MessageDTO;
import com.iceleads.dto.UserDTO;
import com.iceleads.dto.index.IndexProductDTO;
import com.iceleads.dto.index.IndexSearchResultSolrDTO;
import com.iceleads.dto.notification.MergeErrorDTO;
import com.iceleads.dto.staging.StagingProductDTO;
import com.iceleads.dto.web.PaginationDTO;
import com.iceleads.dto.web.index.*;
import com.iceleads.dto.web.staging.NewSkuDTO;
import com.iceleads.dto.web.staging.StagingResultDTO;
import com.iceleads.jobs.exception.FeedImportException;
import com.iceleads.jobs.manager.FeedImportManager;
import com.iceleads.mail.manager.impl.MailManager;
import java.math.BigDecimal;
import java.util.*;
import java.util.logging.Level;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.jboss.logging.Logger;

/**
 *
 * @author Vilaliy K.
 */
@Stateless
@TransactionAttribute(TransactionAttributeType.REQUIRES_NEW)
public class IndexManager implements IIndexManagerRemote {

    private static Logger logger = Logger.getLogger(IndexManager.class);

    @Override
    public IndexResultDTO merge(IndexMergeDTO mergeDTO, String userKey) {
        MessageDTO messageDTO = null;
        Long start = TimeUtil.start();
        Map sessionFactories = DAOSessionUtil.getSessionFactories();

        // iceleads db
        AbstractIceleadsDAOFactory hibernateFactory = DAOFactory.getDAOFactory(sessionFactories).getIceleadsDAOFactory();
        AbstractIceleadsStagingDAOFactory hibernateStagingFactory = DAOFactory.getDAOFactory(sessionFactories).getIceleadsStagingDAOFactory();
        AbstractIceleadsStatDAOFactory hibernateStatFactory = DAOFactory.getDAOFactory(sessionFactories).getIceleadsStatDAOFactory();

        ICoreProductRepositoryDAO coreProductRepositoryDAO = hibernateFactory.getCoreProductRepositoryDAO();
        ICoreProductAlternativeDAO coreProductAlternativeDAO = hibernateFactory.getCoreProductAlternativeDAO();
        ICoreProductEanDAO coreProductEanDAO = hibernateFactory.getCoreProductEanDAO();
        IDeeplinkQualityDAO deeplinkQualityDAO = hibernateFactory.getDeeplinkQualityDAO();
        IDeeplinkQueueItemDAO deeplinkQueueItemDAO = hibernateFactory.getDeeplinkQueueItemDAO();
        IMerchantProductDAO merchantProductDAO = hibernateFactory.getMerchantProductDAO();
        IVendorDAO vendorDAO = hibernateFactory.getVendorDAO();
        ICoreProductMergeDAO coreProductMergeDAO = hibernateFactory.getCoreProductMergeDAO();
        ICoreProductMetadataDAO coreProductMetadataDAO = hibernateFactory.getCoreProductMetadataDAO();
        IUserDAO userDAO = hibernateFactory.getUserDAO();
        IMerchantFeedMapDAO merchantFeedMapDAO = hibernateStagingFactory.getMerchantFeedMapDAO();
        IStatIndexStagingMapDAO statIndexStagingMapDAO = hibernateStatFactory.getStatIndexStagingMapDAO();
        // solr lucene index
        AbstractIceleadsSolrDAOFactory solrFactory = DAOFactory.getDAOFactory(sessionFactories).getIceleadsSolrDAOFactory();
        ISolrIndexProductDAO indexProductDAO = solrFactory.getSolrIndexProductDAO();
        IndexResultDTO baseMergeDTO = mergeDTO.getBaseMergeRow();

        MergeErrorDTO mergeErrorDTO = IndexConverter.getMergeErrorDTO(mergeDTO);

        CoreProductRepositoryDomain baseCoreProduct = coreProductRepositoryDAO.getByID(baseMergeDTO.getProductId());

        if (baseCoreProduct != null) {

            try {
                // update core product repository
                baseCoreProduct.setUserInsId(userDAO.getUserByUserKey(userKey).getId());
                baseCoreProduct = coreProductRepositoryDAO.saveOrUpdate(baseCoreProduct);

                // initialization sets
                Set<IndexAlternativeItemDTO> mpnAlternatives = new HashSet<IndexAlternativeItemDTO>();
                Set<IndexAlternativeItemDTO> eans = new HashSet<IndexAlternativeItemDTO>();
                Set<IndexAlternativeItemDTO> tags = new HashSet<IndexAlternativeItemDTO>();
                Map<Integer, Set<IndexAlternativeItemDTO>> productMerges = new HashMap<Integer, Set<IndexAlternativeItemDTO>>();

                // add mpn alternatives, eans, core ids
                mpnAlternatives.addAll(baseMergeDTO.getMpns());
                eans.addAll(baseMergeDTO.getEans());

                Long mergeTime = TimeUtil.start();

                for (IndexResultDTO mergeItemDTO : mergeDTO.getMergedRows()) {

                    if (!baseMergeDTO.equals(mergeItemDTO)) {
                        mpnAlternatives.addAll(mergeItemDTO.getMpns());
                        eans.addAll(mergeItemDTO.getEans());
                        tags.addAll(mergeItemDTO.getTags());

                        CoreProductRepositoryDomain mergeCoreProduct = coreProductRepositoryDAO.getByID(mergeItemDTO.getProductId());
                        if (mergeCoreProduct != null) {

                            merchantProductDAO.updateMerchantProduct(mergeCoreProduct, baseCoreProduct);
                            coreProductEanDAO.updateProductEan(mergeCoreProduct, baseCoreProduct);
                            coreProductAlternativeDAO.updateProductAlternative(mergeCoreProduct, baseCoreProduct);
                            deeplinkQualityDAO.updateDeeplinkQuality(mergeCoreProduct, baseCoreProduct);
                            deeplinkQueueItemDAO.updateDeeplinkQueue(mergeCoreProduct, baseCoreProduct);
                            merchantFeedMapDAO.updateMerchantFeedMap(mergeCoreProduct, baseCoreProduct);

                            coreProductMergeDAO.updateProductMerge(mergeCoreProduct, baseCoreProduct);
                            if (productMerges.containsKey(mergeCoreProduct.getVendor().getId())) {
                                productMerges.get(mergeCoreProduct.getVendor().getId()).addAll(mergeItemDTO.getMpns());
                            } else {
                                productMerges.put(mergeCoreProduct.getVendor().getId(), new HashSet<IndexAlternativeItemDTO>(mergeItemDTO.getMpns()));
                            }

                            // when we delete merged core_product_repository record by cascade should be delete core_product_ean, core_product_alternative, deeplink_queue,
                            // deeplink_quality, core_product_merge, core_product_metadata, core_product_tag
                            coreProductRepositoryDAO.delete(mergeCoreProduct);
                        }
                    }
                }

                logger.trace("IndexManager:merge:merge time execute " + TimeUtil.snapshot(mergeTime) + " ms.");

                Long insMerge = TimeUtil.start();

                for (Integer vendorId : productMerges.keySet()) {
                    Set<IndexAlternativeItemDTO> mpns = productMerges.get(vendorId);
                    for (IndexAlternativeItemDTO indexAlternativeItem : mpns) {
                        VendorDomain vendor = vendorDAO.getByID(vendorId);
                        CoreProductMergeDomain coreProductMergeDomain = coreProductMergeDAO.getCoreProductMerge(indexAlternativeItem.getAlternativeValue(), vendor);
                        if (coreProductMergeDomain == null) {
                            coreProductMergeDomain = new CoreProductMergeDomain();
                            coreProductMergeDomain.setCoreProductRepository(baseCoreProduct);
                            coreProductMergeDomain.setDateMerge(new Date());
                            coreProductMergeDomain.setMpnMerge(indexAlternativeItem.getAlternativeValue());
                            coreProductMergeDomain.setVendor(vendor);
                            coreProductMergeDAO.save(coreProductMergeDomain);
                        }
                    }
                }

                logger.trace("IndexManager:merge:ins merge time execute " + TimeUtil.snapshot(insMerge) + " ms.");

                // update base metadata
                CoreProductMetadataDomain baseMetadataDomain = coreProductMetadataDAO.getProductMetadata(baseCoreProduct);
                Date firsrDateOffer = merchantProductDAO.getFirstDateOffer(baseCoreProduct);
                if (baseMetadataDomain != null && firsrDateOffer != null) {
                    baseMetadataDomain.setOnMarketSince(firsrDateOffer);
                    baseMetadataDomain.setQtyOffer(merchantProductDAO.getCountOffers(baseCoreProduct));
                    coreProductMetadataDAO.update(baseMetadataDomain);

                    baseMergeDTO.setOffersCount(new BigDecimal(baseMetadataDomain.getQtyOffer()));
                    baseMergeDTO.setMarketSince(baseMetadataDomain.getOnMarketSince());
                }

                baseMergeDTO.setEans(new ArrayList<IndexAlternativeItemDTO>(eans));
                baseMergeDTO.setMpns(new ArrayList<IndexAlternativeItemDTO>(mpnAlternatives));
                baseMergeDTO.setTags(new ArrayList<IndexAlternativeItemDTO>(tags));

                // save statistic staging map
                StatIndexStagingMapDomain statIndexStagingMergeDomain = new StatIndexStagingMapDomain();
                statIndexStagingMergeDomain.setVendorId(baseCoreProduct.getVendor().getId());
                statIndexStagingMergeDomain.setVendorName(baseCoreProduct.getVendor().getName());
                statIndexStagingMergeDomain.setCategoryId(baseCoreProduct.getCategory().getId());
                statIndexStagingMergeDomain.setCategoryName(baseCoreProduct.getCategory().getName());
                statIndexStagingMergeDomain.setMpn(baseCoreProduct.getMpn());
                statIndexStagingMergeDomain.setDescription(baseCoreProduct.getModelName());
                statIndexStagingMergeDomain.setCoreProductRepositoryId(baseCoreProduct.getId());
                statIndexStagingMergeDomain.setUserInsertId(userDAO.getUserByUserKey(userKey).getId());
                statIndexStagingMergeDomain.setUserLogin(userDAO.getUserByUserKey(userKey).getLogin());
                statIndexStagingMergeDomain.setDateIns(new Date());
                statIndexStagingMergeDomain.setTransactionCode(String.valueOf(Math.abs(baseCoreProduct.hashCode())));
                statIndexStagingMergeDomain.setRefActivityTypeId(RefActivityTypeEnum.MERGED.ordinal());
                statIndexStagingMapDAO.saveOrUpdate(statIndexStagingMergeDomain);


                //update base merge record with new, will send to index for updates
                try {
                    Long indexMerge = TimeUtil.start();
                    indexProductDAO.updateMergedIndexProduct(mergeDTO);
                    logger.trace("IndexManager:merge:solr index merge time execute " + TimeUtil.snapshot(indexMerge) + " ms.");
                } catch (Exception ex) {
                    logger.debug("IndexManager:getSolrIndex: ", ex);
                    logger.error("IndexManager:getSolrIndex: " + ex.getMessage());
                    messageDTO = new MessageDTO();
                    messageDTO.setCodeMessage("Error on solr index merge");
                    messageDTO.setMessage(ex.getMessage());
                    mergeErrorDTO.getExceptionDTO().setMessage(ex.getMessage());
                    mergeErrorDTO.getExceptionDTO().setStackTraces(ex.getStackTrace());
                    MailManager.send(mergeErrorDTO, NotificationKeyEnum.MERGE_NOTIFICATION_ERROR.getValue(), null, null);
                }
            } catch (Exception ex) {
                logger.debug("IndexManager:merge: ", ex);
                logger.error("IndexManager:merge: " + ex.getMessage());
                messageDTO = new MessageDTO();
                messageDTO.setCodeMessage("Error on database merge");
                messageDTO.setMessage(ex.getMessage());
                mergeErrorDTO.getExceptionDTO().setMessage(ex.getMessage());
                mergeErrorDTO.getExceptionDTO().setStackTraces(ex.getStackTrace());
                MailManager.send(mergeErrorDTO, NotificationKeyEnum.MERGE_NOTIFICATION_ERROR.getValue(), null, null);
            }
        } else {
            mergeErrorDTO.getExceptionDTO().setMessage("Product " + baseMergeDTO.getProductId() + " where in merge dto but not exist in database");
            MailManager.send(mergeErrorDTO, NotificationKeyEnum.MERGE_NOTIFICATION_ERROR.getValue(), null, null);
        }
        logger.trace("IndexManager:merge:out time execute " + TimeUtil.snapshot(start) + " ms.");

        baseMergeDTO.setMessageDTO(messageDTO);
        return baseMergeDTO;
    }

    /**
     * @author Vitaliy K.
     */
    @Override
    public List<IndexOfferDTO> getOffers(Integer coreProductId) {
        Long start = TimeUtil.start();
        Map sessionFactories = DAOSessionUtil.getSessionFactories();
        AbstractIceleadsDAOFactory hibernateFactory = DAOFactory.getDAOFactory(sessionFactories).getIceleadsDAOFactory();
        IMerchantProductUrlDAO merchantProductUrlDAO = hibernateFactory.getMerchantUrlDAO();
        List<IndexOfferDTO> offerDTOs = new ArrayList<IndexOfferDTO>();
        try {
            List<MerchantProductUrlDomain> merchantProductUrlDomains = merchantProductUrlDAO.getMerchantUrl(coreProductId);
            List<MerchantProductUrlDomain> distinctMerchantProductUrlDomains = new ArrayList<MerchantProductUrlDomain>();
            HashMap<Integer, MerchantDomain> merchantMap = new HashMap<Integer, MerchantDomain>();
            for (MerchantProductUrlDomain urlDomain : merchantProductUrlDomains) {
                MerchantDomain merchantDomain = urlDomain.getMerchantProduct().getMerchant();
                if (!merchantMap.containsKey(merchantDomain.getId())) {
                    merchantMap.put(merchantDomain.getId(), merchantDomain);
                    distinctMerchantProductUrlDomains.add(urlDomain);
                }
            }
            offerDTOs = IndexConverter.getIndexOfferDTOs(distinctMerchantProductUrlDomains, merchantMap);
        } catch (Exception ex) {
            logger.debug("IndexManager:getOffers: ", ex);
            logger.error("IndexManager:getOffers: " + ex.getMessage());
        }
        logger.trace("IndexManager:getOffers:out time execute " + TimeUtil.snapshot(start) + " ms.");
        return offerDTOs;
    }

    @Override
    public PaginationDTO<IndexResultDTO> searchIndex(PaginationDTO<IndexResultDTO> paginationDTO, IndexParamDTO paramDTO) {
        Long start = TimeUtil.start();

        // solr lucene index
        Map sessionFactories = DAOSessionUtil.getSessionFactories();
        AbstractIceleadsSolrDAOFactory solrFactory = DAOFactory.getDAOFactory(sessionFactories).getIceleadsSolrDAOFactory();
        ISolrIndexProductDAO indexProductDAO = solrFactory.getSolrIndexProductDAO();

        String countryName = null;
        String merchantName = null;
        String vendorName = null;
        String categoryName = null;
        String searchString = null;
        String prodCode = null;
        String mpn = null;
        Boolean containMpn = true;
        String ean = null;
        Boolean containEan = true;
        String desc = null;
        Boolean containDesc = true;
        Integer maxOffer = null;
        Integer minOffer = null;
        String dateMarketSince = null;
        String tag = null;
        Integer coreProductRepositoryId = null;
        String optionTag = null;

        Boolean runSearch = false;

        List<IndexResultDTO> indexResultDTOs = new ArrayList<IndexResultDTO>();
        try {

            if (paramDTO.getCoreProductRepositoryId() != null) {
                coreProductRepositoryId = paramDTO.getCoreProductRepositoryId();
                runSearch = true;
            }

            if (paramDTO.getCountryName() != null && !paramDTO.getCountryName().toLowerCase().trim().isEmpty()) {
                countryName = ClientUtils.escapeQueryChars(paramDTO.getCountryName().toLowerCase().trim());
                runSearch = true;
            }

            if (paramDTO.getMerchantName() != null && !paramDTO.getMerchantName().toLowerCase().trim().isEmpty()) {
                merchantName = ClientUtils.escapeQueryChars(paramDTO.getMerchantName().toLowerCase().trim());
                runSearch = true;
            }

            if (paramDTO.getVendorName() != null && !paramDTO.getVendorName().toLowerCase().trim().isEmpty()) {
                vendorName = ClientUtils.escapeQueryChars(paramDTO.getVendorName().toLowerCase().trim());
                runSearch = true;
            }

            if (paramDTO.getCategoryName() != null && !paramDTO.getCategoryName().toLowerCase().trim().isEmpty()) {
                categoryName = ClientUtils.escapeQueryChars(paramDTO.getCategoryName());
                runSearch = true;
            }

            if (paramDTO.getSearchString() != null) {
                searchString = ClientUtils.escapeQueryChars(paramDTO.getSearchString());
                runSearch = true;
            }

            if (!(paramDTO.getProdCode() == null || paramDTO.getProdCode().isEmpty())) {
                prodCode = ClientUtils.escapeQueryChars(paramDTO.getProdCode());
                runSearch = true;
            }

            if (!(paramDTO.getMpn() == null || paramDTO.getMpn().isEmpty())) {
                mpn = ClientUtils.escapeQueryChars(paramDTO.getMpn());
                if (paramDTO.getContainMpn().equals(RestrictValueEnum.VALUE_DOES_NOT_CONTAION.getValue())) {
                    containMpn = false;
                }
                runSearch = true;
            }

            if (!(paramDTO.getEan() == null || paramDTO.getEan().isEmpty())) {
                ean = ClientUtils.escapeQueryChars(paramDTO.getEan());
                if (paramDTO.getContainEan().equals(RestrictValueEnum.VALUE_DOES_NOT_CONTAION.getValue())) {
                    containEan = false;
                }
                runSearch = true;
            }

            if (!(paramDTO.getDesc() == null || paramDTO.getDesc().isEmpty())) {
                desc = ClientUtils.escapeQueryChars(paramDTO.getDesc());
                if (paramDTO.getContainDesc().equals(RestrictValueEnum.VALUE_DOES_NOT_CONTAION.getValue())) {
                    containDesc = false;
                }
                runSearch = true;
            }

            if (paramDTO.getMaxOffer() != null) {
                maxOffer = paramDTO.getMaxOffer();
            }

            if (paramDTO.getMinOffer() != null) {
                minOffer = paramDTO.getMinOffer();
            }

            if (paramDTO.getMarketSince() != null) {
                dateMarketSince = DateFormatUtil.getSolrDate(paramDTO.getMarketSince());
            }

            if (!(paramDTO.getTag() == null || paramDTO.getTag().isEmpty())) {
                tag = ClientUtils.escapeQueryChars(paramDTO.getTag());
                runSearch = true;
            }

            if (paramDTO.getOptionTag() != null) {
                optionTag = paramDTO.getOptionTag();
                runSearch = true;
            }

            if (runSearch) {
                Object[] result = indexProductDAO.getCoreProductByIndexParams(coreProductRepositoryId, countryName, merchantName, vendorName, categoryName, prodCode, mpn, containMpn, ean, containEan, desc, containDesc, paramDTO.getOnMarket(), searchString, maxOffer, minOffer, dateMarketSince, tag, optionTag, paginationDTO.getFirstIndex(), paginationDTO.getResultPerPage(), paginationDTO.getSortOrder());
                paginationDTO.setResultCount((Integer) result[0]);
                indexResultDTOs = IndexConverter.getIndexResultDTOs((List<IndexSearchResultSolrDTO>) result[1]);
            }
        } catch (Exception ex) {
            logger.debug("IndexManager:searchIndex: ", ex);
            logger.error("IndexManager:searchIndex: " + ex.getMessage());
        }
        paginationDTO.setPageItems(indexResultDTOs);
        logger.trace("IndexManager:searchIndex:out time execute " + TimeUtil.snapshot(start) + " ms.");
        return paginationDTO;
    }

    @Override
    public Integer searchIndexCount(IndexParamDTO paramDTO) {
        Long start = TimeUtil.start();

        Integer rowCount = 0;

        // solr lucene index
        Map sessionFactories = DAOSessionUtil.getSessionFactories();
        AbstractIceleadsSolrDAOFactory solrFactory = DAOFactory.getDAOFactory(sessionFactories).getIceleadsSolrDAOFactory();
        ISolrIndexProductDAO indexProductDAO = solrFactory.getSolrIndexProductDAO();

        String countryName = null;
        String merchantName = null;
        String vendorName = null;
        String categoryName = null;
        String searchString = null;
        String prodCode = null;
        String mpn = null;
        Boolean containMpn = true;
        String ean = null;
        Boolean containEan = true;
        String desc = null;
        Boolean containDesc = true;
        Integer maxOffer = null;
        Integer minOffer = null;
        String dateMarketSince = null;
        String tag = null;
        Integer coreProductRepositoryId = null;
        String optionTag = null;

        try {
            if (paramDTO.getCoreProductRepositoryId() != null) {
                coreProductRepositoryId = paramDTO.getCoreProductRepositoryId();
            }

            if (paramDTO.getCountryName() != null && !paramDTO.getCountryName().toLowerCase().trim().isEmpty()) {
                countryName = ClientUtils.escapeQueryChars(paramDTO.getCountryName().toLowerCase().trim());
            }

            if (paramDTO.getMerchantName() != null && !paramDTO.getMerchantName().toLowerCase().trim().isEmpty()) {
                merchantName = ClientUtils.escapeQueryChars(paramDTO.getMerchantName().toLowerCase().trim());
            }

            if (paramDTO.getVendorName() != null && !paramDTO.getVendorName().toLowerCase().trim().isEmpty()) {
                vendorName = ClientUtils.escapeQueryChars(paramDTO.getVendorName().toLowerCase().trim());
            }

            if (paramDTO.getCategoryName() != null && !paramDTO.getCategoryName().toLowerCase().trim().isEmpty()) {
                categoryName = ClientUtils.escapeQueryChars(paramDTO.getCategoryName().toLowerCase().trim());
            }

            if (paramDTO.getSearchString() != null) {
                searchString = ClientUtils.escapeQueryChars(paramDTO.getSearchString());
            }

            if (!(paramDTO.getProdCode() == null || paramDTO.getProdCode().isEmpty())) {
                prodCode = ClientUtils.escapeQueryChars(paramDTO.getProdCode());
            }

            if (!(paramDTO.getMpn() == null || paramDTO.getMpn().isEmpty())) {
                mpn = ClientUtils.escapeQueryChars(paramDTO.getMpn());
                if (paramDTO.getContainMpn().equals(RestrictValueEnum.VALUE_DOES_NOT_CONTAION.getValue())) {
                    containMpn = false;
                }
            }

            if (!(paramDTO.getEan() == null || paramDTO.getEan().isEmpty())) {
                ean = ClientUtils.escapeQueryChars(paramDTO.getEan());
                if (paramDTO.getContainEan().equals(RestrictValueEnum.VALUE_DOES_NOT_CONTAION.getValue())) {
                    containEan = false;
                }
            }

            if (!(paramDTO.getDesc() == null || paramDTO.getDesc().isEmpty())) {
                desc = ClientUtils.escapeQueryChars(paramDTO.getDesc());
                if (paramDTO.getContainDesc().equals(RestrictValueEnum.VALUE_DOES_NOT_CONTAION.getValue())) {
                    containDesc = false;
                }
            }

            if (paramDTO.getMaxOffer() != null) {
                maxOffer = paramDTO.getMaxOffer();
            }

            if (paramDTO.getMinOffer() != null) {
                minOffer = paramDTO.getMinOffer();
            }

            if (paramDTO.getMarketSince() != null) {
                dateMarketSince = DateFormatUtil.getSolrDate(paramDTO.getMarketSince());
            }

            if (!(paramDTO.getTag() == null || paramDTO.getTag().isEmpty())) {
                tag = ClientUtils.escapeQueryChars(paramDTO.getTag());
            }

            if (paramDTO.getOptionTag() != null) {
                optionTag = paramDTO.getOptionTag();
            }
            rowCount = indexProductDAO.getCountCoreProductByIndexParams(coreProductRepositoryId, countryName, merchantName, vendorName, categoryName, prodCode, mpn, containMpn, ean, containEan, desc, containDesc, paramDTO.getOnMarket(), searchString, maxOffer, minOffer, dateMarketSince, tag, optionTag);
        } catch (Exception ex) {
            logger.debug("IndexManager:searchIndexCount: ", ex);
            logger.error("IndexManager:searchIndexCount: " + ex.getMessage());
        }

        logger.trace("IndexManager:searchIndexCount:row count [" + rowCount + "]: out time execute " + TimeUtil.snapshot(start) + " ms.");
        return rowCount;
    }

    @Override
    public PaginationDTO<IndexNativeResultDTO> nativeSearchIndex(PaginationDTO<IndexNativeResultDTO> paginationDTO, IndexNativeParamDTO paramDTO) {
        Long start = TimeUtil.start();

        // solr lucene index
        Map sessionFactories = DAOSessionUtil.getSessionFactories();
        AbstractIceleadsSolrDAOFactory solrFactory = DAOFactory.getDAOFactory(sessionFactories).getIceleadsSolrDAOFactory();
        ISolrIndexProductDAO indexProductDAO = solrFactory.getSolrIndexProductDAO();
        String mpn = null;
        String ean = null;
        String prodCode = null;
        String desc = null;
        String categoryName = null;
        String vendorName = null;

        List<IndexNativeResultDTO> indexResultDTOs = new ArrayList<IndexNativeResultDTO>();
        Integer rowCount = 0;

        try {
            if (!(paramDTO.getMpn() == null || paramDTO.getMpn().toLowerCase().trim().isEmpty())) {
                mpn = ClientUtils.escapeQueryChars(paramDTO.getMpn().toLowerCase().trim());
            }
            if (!(paramDTO.getEan() == null || paramDTO.getEan().toLowerCase().trim().isEmpty())) {
                ean = ClientUtils.escapeQueryChars(paramDTO.getEan().toLowerCase().trim());
            }
            if (!(paramDTO.getProdCode() == null || paramDTO.getProdCode().toLowerCase().trim().isEmpty())) {
                prodCode = ClientUtils.escapeQueryChars(paramDTO.getProdCode().toLowerCase().trim());
            }
            if (!(paramDTO.getDesc() == null || paramDTO.getDesc().toLowerCase().trim().isEmpty())) {
                desc = ClientUtils.escapeQueryChars(paramDTO.getDesc().toLowerCase().trim());
            }
            if (!(paramDTO.getCategoryName() == null || paramDTO.getCategoryName().toLowerCase().trim().isEmpty())) {
                categoryName = ClientUtils.escapeQueryChars(paramDTO.getCategoryName().toLowerCase().trim());
            }
            if (!(paramDTO.getVendorName() == null || paramDTO.getVendorName().toLowerCase().trim().isEmpty())) {
                vendorName = ClientUtils.escapeQueryChars(paramDTO.getVendorName().toLowerCase().trim());
            }
            Object[] result = indexProductDAO.getCoreProductByIndexParams(null, null, null, vendorName, categoryName, prodCode, mpn, Boolean.TRUE, ean, Boolean.TRUE, desc, Boolean.TRUE, Boolean.FALSE, null, null, null, null, null, null, paginationDTO.getFirstIndex(), paginationDTO.getResultPerPage(), paginationDTO.getSortOrder());
            rowCount = (Integer) result[0];
            indexResultDTOs = IndexConverter.getNativeIndexResultDTOs((List<IndexSearchResultSolrDTO>) result[1]);
        } catch (Exception ex) {
            logger.debug("IndexManager:nativeSearchIndex: ", ex);
            logger.error("IndexManager:nativeSearchIndex: " + ex.getMessage());
        }
        paginationDTO.setResultCount(rowCount);
        paginationDTO.setPageItems(indexResultDTOs);
        logger.trace("IndexManager:nativeSearchIndex:out time execute " + TimeUtil.snapshot(start) + " ms.");
        return paginationDTO;
    }

    @Override
    public Integer nativeSearchIndexCount(IndexNativeParamDTO paramDTO) {
        Long start = TimeUtil.start();
        Integer rowCount = 0;
        // solr lucene index
        Map sessionFactories = DAOSessionUtil.getSessionFactories();
        AbstractIceleadsSolrDAOFactory solrFactory = DAOFactory.getDAOFactory(sessionFactories).getIceleadsSolrDAOFactory();
        ISolrIndexProductDAO indexProductDAO = solrFactory.getSolrIndexProductDAO();
        String mpn = null;
        String ean = null;
        String prodCode = null;
        String desc = null;
        String categoryName = null;
        String vendorName = null;
        try {
            if (!(paramDTO.getMpn() == null || paramDTO.getMpn().toLowerCase().trim().isEmpty())) {
                mpn = ClientUtils.escapeQueryChars(paramDTO.getMpn().toLowerCase().trim());
            }
            if (!(paramDTO.getEan() == null || paramDTO.getEan().toLowerCase().trim().isEmpty())) {
                ean = ClientUtils.escapeQueryChars(paramDTO.getEan().toLowerCase().trim());
            }
            if (!(paramDTO.getProdCode() == null || paramDTO.getProdCode().toLowerCase().trim().isEmpty())) {
                prodCode = ClientUtils.escapeQueryChars(paramDTO.getProdCode().toLowerCase().trim());
            }
            if (!(paramDTO.getDesc() == null || paramDTO.getDesc().toLowerCase().trim().isEmpty())) {
                desc = ClientUtils.escapeQueryChars(paramDTO.getDesc().toLowerCase().trim());
            }
            if (!(paramDTO.getCategoryName() == null || paramDTO.getCategoryName().toLowerCase().trim().isEmpty())) {
                categoryName = ClientUtils.escapeQueryChars(paramDTO.getCategoryName().toLowerCase().trim());
            }
            if (!(paramDTO.getVendorName() == null || paramDTO.getVendorName().toLowerCase().trim().isEmpty())) {
                vendorName = ClientUtils.escapeQueryChars(paramDTO.getVendorName().toLowerCase().trim());
            }
            rowCount = indexProductDAO.getCountCoreProductByIndexParams(null, null, null, vendorName, categoryName, prodCode, mpn, Boolean.TRUE, ean, Boolean.TRUE, desc, Boolean.TRUE, Boolean.FALSE, null, null, null, null, null, null);
        } catch (Exception e) {
            logger.debug("IndexManager:nativeSearchIndexCount: ", e);
            logger.error("IndexManager:nativeSearchIndexCount: " + e.getMessage());
        }
        logger.trace("IndexManager:nativeSearchIndexCount:row count [" + rowCount + "]: out time execute " + TimeUtil.snapshot(start) + " ms.");
        return rowCount;
    }

    @Override
    public NewSkuDTO createNewSku(NewSkuDTO newSkuDTO, UserDTO userDTO) {
        Map sessionFactories = DAOSessionUtil.getSessionFactories();

        // iceleads db
        AbstractIceleadsDAOFactory hibernateFactory = DAOFactory.getDAOFactory(sessionFactories).getIceleadsDAOFactory();
        ICoreProductRepositoryDAO coreProductRepositoryDAO = hibernateFactory.getCoreProductRepositoryDAO();
        ICoreProductAlternativeDAO coreProductAlternativeDAO = hibernateFactory.getCoreProductAlternativeDAO();
        ICoreProductEanDAO coreProductEanDAO = hibernateFactory.getCoreProductEanDAO();
        ICategoryDAO categoryDAO = hibernateFactory.getCategoryDAO();
        IVendorDAO vendorDAO = hibernateFactory.getVendorDAO();

        // iceleads_stat db
        AbstractIceleadsStatDAOFactory hibernateStatFactory = DAOFactory.getDAOFactory(sessionFactories).getIceleadsStatDAOFactory();
        IStatIndexStagingMapDAO statIndexStagingMapDAO = hibernateStatFactory.getStatIndexStagingMapDAO();

        // iceleads_staging db
        AbstractIceleadsStagingDAOFactory hibernateStagingFactory = DAOFactory.getDAOFactory(sessionFactories).getIceleadsStagingDAOFactory();
        IMerchantFeedMapDAO merchantFeedMapDAO = hibernateStagingFactory.getMerchantFeedMapDAO();

        // solr lucene index
        AbstractIceleadsSolrDAOFactory solrFactory = DAOFactory.getDAOFactory(sessionFactories).getIceleadsSolrDAOFactory();
        ISolrIndexProductDAO indexProductDAO = solrFactory.getSolrIndexProductDAO();

        AbstractIceleadsSolrDAOFactory stagingSolrFactory = DAOFactory.getDAOFactory(sessionFactories).getStagingSolrDAOFactory();
        ISolrStagingProductDAO stagingProductDAO = stagingSolrFactory.getSolrStagingProductDAO();

        VendorDomain vendorDomain = vendorDAO.getByName(newSkuDTO.getVendorName());
        CategoryDomain categoryDomain = categoryDAO.getCategory(newSkuDTO.getCategoryName());

        //check if product does not exist
        CoreProductRepositoryDomain coreProductRepositoryDomain = coreProductRepositoryDAO.getCoreProduct(newSkuDTO.getMpn(), vendorDomain);
        if (coreProductRepositoryDomain == null) {
            // Create new sku
            coreProductRepositoryDomain = new CoreProductRepositoryDomain();
            coreProductRepositoryDomain.setCategory(categoryDomain);
            coreProductRepositoryDomain.setVendor(vendorDomain);
            coreProductRepositoryDomain.setMpn(newSkuDTO.getMpn());
            coreProductRepositoryDomain.setDateIns(new Date());
            coreProductRepositoryDomain.setUserInsId((userDTO != null) ? userDTO.getUserId() : null);
            categoryDomain.setDateUpdate(new Date());

            //save CoreProductRepository
            coreProductRepositoryDomain = coreProductRepositoryDAO.saveOrUpdate(coreProductRepositoryDomain);
            coreProductRepositoryDomain = coreProductRepositoryDAO.getByID(coreProductRepositoryDomain.getId());
            newSkuDTO.setId(coreProductRepositoryDomain.getId());

            Set<String> eanSet = new HashSet<String>();
            Set<String> mpnSet = new HashSet<String>();

            // -------------- Alternative MPNs
            Set<CoreProductAlternativeDomain> alternativeMpns = new HashSet<CoreProductAlternativeDomain>();

            //create bacis MPN
            CoreProductAlternativeDomain alternativeDomain = new CoreProductAlternativeDomain();
            alternativeDomain.setMpnAlternative(newSkuDTO.getMpn());
            alternativeDomain.setDateIns(new Date());
            alternativeDomain.setUserIns((userDTO != null) ? userDTO.getUserId() : null);
            alternativeDomain.setCoreProductRepository(coreProductRepositoryDomain);
            alternativeMpns.add(alternativeDomain);
            coreProductAlternativeDAO.save(alternativeDomain);
            mpnSet.add(newSkuDTO.getMpn());

            //Add original MPN if it diferent from NewMPN
            List<String> stagingIds = new ArrayList<String>();
            List<StagingProductDTO> stagingProductDTOs = new ArrayList<StagingProductDTO>();
            for (StagingResultDTO stagingResultDTO : newSkuDTO.getStagingResultDTOs()) {
                stagingIds.add(stagingResultDTO.getId());
            }
            try {
                // select stagingProductDTOs from Solr 
                stagingProductDTOs = stagingProductDAO.getListByIDs(stagingIds);
                for (StagingProductDTO stagingProductDTO : stagingProductDTOs) {
                    if (stagingProductDTO.getOrgMpn() != null && !stagingProductDTO.getOrgMpn().equalsIgnoreCase(newSkuDTO.getMpn())) {
                        alternativeDomain = new CoreProductAlternativeDomain();
                        alternativeDomain.setMpnAlternative(stagingProductDTO.getOrgMpn());
                        alternativeDomain.setDateIns(new Date());
                        alternativeDomain.setUserIns((userDTO != null) ? userDTO.getUserId() : null);
                        alternativeDomain.setCoreProductRepository(coreProductRepositoryDomain);
                        alternativeMpns.add(alternativeDomain);
                        coreProductAlternativeDAO.save(alternativeDomain);
                        mpnSet.add(newSkuDTO.getMpn());
                    }
                }
            } catch (SolrServerException ex) {
                logger.debug("IndexManager:createNewSku: ", ex);
                logger.error("IndexManager:createNewSku: SolrServerException  " + ex.getMessage());
            }

            // create EANs 
            CoreProductEanDomain coreProductEanDomain = new CoreProductEanDomain();
            coreProductEanDomain.setEan(newSkuDTO.getEan());
            coreProductEanDomain.setUserIns((userDTO != null) ? userDTO.getUserId() : null);
            coreProductEanDomain.setDateIns(new Date());
            coreProductEanDomain.setCoreProductRepository(coreProductRepositoryDomain);
            Set<CoreProductEanDomain> eans = new HashSet<CoreProductEanDomain>();
            eanSet.add(newSkuDTO.getEan());
            eans.add(coreProductEanDomain);
            coreProductEanDAO.save(coreProductEanDomain);

            //createa coreProductRepository 
            coreProductRepositoryDomain.setCoreProductAlternatives(alternativeMpns);
            coreProductRepositoryDomain.setCoreProductEans(eans);
            coreProductRepositoryDomain = coreProductRepositoryDAO.saveOrUpdate(coreProductRepositoryDomain);

            // Create new solr index product 
            IndexProductDTO indexProductDTO = new IndexProductDTO();
            indexProductDTO.setCategoryId(categoryDomain.getId());
            indexProductDTO.setCategoryName(categoryDomain.getName());
            indexProductDTO.setVendorId(vendorDomain.getId());
            indexProductDTO.setVendorName(vendorDomain.getName());
            indexProductDTO.setId(coreProductRepositoryDomain.getId());
            indexProductDTO.setMpn(coreProductRepositoryDomain.getMpn());
            indexProductDTO.setMpnAlternative(new ArrayList<String>(mpnSet));
            indexProductDTO.setEan(new ArrayList<String>(eanSet));
            indexProductDTO.setTag(new ArrayList<String>());
            indexProductDTO.setAffiliateId(new ArrayList<Integer>());
            indexProductDTO.setQty(new Integer(0));
            indexProductDTO.setCountryName(new ArrayList<String>());
            indexProductDTO.setMerchantName(new ArrayList<String>());
            indexProductDTO.setModelName((coreProductRepositoryDomain.getModelName() != null) ? coreProductRepositoryDomain.getModelName() : new String());
            indexProductDTO.setMerchantSince(new Date());
            try {
                indexProductDAO.save(indexProductDTO);
            } catch (Exception ex) {
                logger.debug("IndexManager:createNewSku: ", ex);
                logger.error("IndexManager:createNewSku: " + ex.getMessage());
            }

            //Process mis-data mapping
            for (StagingProductDTO stagingProductDTO : stagingProductDTOs) {

                if (stagingProductDTO.getDeepLink() != null
                        && (stagingProductDTO.getOrgVendorName() == null
                        || stagingProductDTO.getOrgMpn() == null)) {

                    MerchantFeedMapDomain merchantFeedMapDomain = merchantFeedMapDAO.getMerchantFeedMap(stagingProductDTO.getMerchantId(), MessageDigestUtil.getHashHex(stagingProductDTO.getDeepLink()));
                    if (merchantFeedMapDomain == null) {
                        merchantFeedMapDomain = new MerchantFeedMapDomain();
                        merchantFeedMapDomain.setCoreProductRepositoryId(coreProductRepositoryDomain.getId());
                        merchantFeedMapDomain.setMerchantId(stagingProductDTO.getMerchantId());
                        merchantFeedMapDomain.setUserIns(userDTO.getUserId());
                        merchantFeedMapDomain.setDateIns(new Date());
                    }
                    merchantFeedMapDomain.setDateUpdate(new Date());
                    merchantFeedMapDomain.setOfferKey(MessageDigestUtil.getHashHex(stagingProductDTO.getDeepLink()));
                    merchantFeedMapDomain = merchantFeedMapDAO.saveOrUpdate(merchantFeedMapDomain);
                }
            }

            Integer transactionCode = Math.abs(stagingIds.hashCode());

            // save statistic staging map
            for (StagingProductDTO stagingProductDTO : stagingProductDTOs) {
                StatIndexStagingMapDomain statIndexStagingMergeDomain = new StatIndexStagingMapDomain();
                statIndexStagingMergeDomain.setRefCountryId(stagingProductDTO.getRefCountryId());
                statIndexStagingMergeDomain.setRefCountryName(stagingProductDTO.getCountryName());
                statIndexStagingMergeDomain.setMerchantId(stagingProductDTO.getMerchantId());
                statIndexStagingMergeDomain.setMerchantName(stagingProductDTO.getMerchantName());
                statIndexStagingMergeDomain.setVendorId((stagingProductDTO.getVendorId() != null) ? stagingProductDTO.getVendorId() : indexProductDTO.getVendorId());
                statIndexStagingMergeDomain.setVendorName((stagingProductDTO.getVendorName() != null) ? stagingProductDTO.getVendorName() : indexProductDTO.getVendorName());
                statIndexStagingMergeDomain.setCategoryId((stagingProductDTO.getCategoryId() != null) ? stagingProductDTO.getCategoryId() : indexProductDTO.getCategoryId());
                statIndexStagingMergeDomain.setCategoryName((stagingProductDTO.getCategoryName() != null) ? stagingProductDTO.getCategoryName() : indexProductDTO.getCategoryName());
                statIndexStagingMergeDomain.setMpn((stagingProductDTO.getMpn() != null) ? stagingProductDTO.getMpn() : indexProductDTO.getMpn());
                statIndexStagingMergeDomain.setDescription(indexProductDTO.getModelName());
                statIndexStagingMergeDomain.setOrgCategoryName(stagingProductDTO.getOrgCategoryName());
                statIndexStagingMergeDomain.setOrgVendorName(stagingProductDTO.getOrgVendorName());
                statIndexStagingMergeDomain.setOrgMpn(stagingProductDTO.getOrgMpn());
                statIndexStagingMergeDomain.setOrgDeeplink(stagingProductDTO.getDeepLink());
                statIndexStagingMergeDomain.setOrgDescription(stagingProductDTO.getOrgForeingDescription());
                statIndexStagingMergeDomain.setCoreProductRepositoryId(indexProductDTO.getId());
                statIndexStagingMergeDomain.setUserInsertId((userDTO != null) ? userDTO.getUserId() : null);
                statIndexStagingMergeDomain.setUserLogin((userDTO != null) ? userDTO.getLogin() : null);
                statIndexStagingMergeDomain.setDateIns(new Date());
                statIndexStagingMergeDomain.setTransactionCode(String.valueOf(transactionCode));
                statIndexStagingMergeDomain.setRefActivityTypeId(RefActivityTypeEnum.NEW_TO_INDEX.ordinal());
                statIndexStagingMapDAO.saveOrUpdate(statIndexStagingMergeDomain);
            }

            try {
                // Transform and add into master 
                // TODO: redevelop to use EJB. 
                FeedImportManager.getInctance().transformStagingProducts(stagingProductDTOs, transactionCode);
            } catch (FeedImportException ex) {
                logger.debug("IndexManager:createNewSku: ", ex);
                logger.error("IndexManager:createNewSku: " + ex.getMessage());
            }
        } else {
            newSkuDTO.setId(coreProductRepositoryDomain.getId());
        }
        return newSkuDTO;
    }

    @Override
    public Boolean edit(IndexEditDTO indexEditDTO, String userKey) {
        Long start = TimeUtil.start();
        Map sessionFactories = DAOSessionUtil.getSessionFactories();

        AbstractIceleadsDAOFactory hibernateFactory = DAOFactory.getDAOFactory(sessionFactories).getIceleadsDAOFactory();
        ICoreProductRepositoryDAO coreProductRepositoryDAO = hibernateFactory.getCoreProductRepositoryDAO();
        ICoreProductEanDAO coreProductEanDAO = hibernateFactory.getCoreProductEanDAO();
        ICoreProductAlternativeDAO coreProductAlternativeDAO = hibernateFactory.getCoreProductAlternativeDAO();
        ICoreProductTagDAO coreProductTagDAO = hibernateFactory.getCoreProductTagDAO();
        ICategoryDAO categoryDAO = hibernateFactory.getCategoryDAO();
        IUserDAO userDAO = hibernateFactory.getUserDAO();

        AbstractIceleadsStatDAOFactory hibernateStatFactory = DAOFactory.getDAOFactory(sessionFactories).getIceleadsStatDAOFactory();
        IStatIndexStagingMapDAO statIndexStagingMapDAO = hibernateStatFactory.getStatIndexStagingMapDAO();

        AbstractIceleadsSolrDAOFactory solrFactory = DAOFactory.getDAOFactory(sessionFactories).getIceleadsSolrDAOFactory();
        ISolrIndexProductDAO indexProductDAO = solrFactory.getSolrIndexProductDAO();

        AbstractIceleadsSolrDAOFactory stagingSolrFactory = DAOFactory.getDAOFactory(sessionFactories).getStagingSolrDAOFactory();
        ISolrStagingProductDAO stagingProductDAO = stagingSolrFactory.getSolrStagingProductDAO();

        Boolean result = Boolean.FALSE;
        try {
            CategoryDomain newCategoryDomain = null;
            CategoryDomain oldCategoryDomain = null;

            CoreProductRepositoryDomain coreProductRepositoryDomain = coreProductRepositoryDAO.getByID(indexEditDTO.getCoreProductId());


            if (coreProductRepositoryDomain != null) {

                oldCategoryDomain = coreProductRepositoryDomain.getCategory();
                newCategoryDomain = categoryDAO.getCategory(indexEditDTO.getCategoryName());


                List<CoreProductEanDomain> productEanDomains = coreProductEanDAO.getCoreProductEan(coreProductRepositoryDomain);
                List<CoreProductAlternativeDomain> productMpnDomains = coreProductAlternativeDAO.getCoreProductAlternative(coreProductRepositoryDomain);
                List<CoreProductTagDomain> productTagDomains = coreProductTagDAO.getCoreProductTagList(coreProductRepositoryDomain);

                // delete removed EANs from db
                if (!productEanDomains.isEmpty()) {
                    for (CoreProductEanDomain coreProductEanDomain : productEanDomains) {
                        if (!indexEditDTO.getEans().contains(coreProductEanDomain.getEan())) {
                            coreProductEanDAO.delete(coreProductEanDomain);
                        }
                    }
                }
                logger.trace("IndexManager:edit:remove old eans for base product " + TimeUtil.snapshot(start) + " ms.");

                // delete removed MPNs from db
                if (!productMpnDomains.isEmpty()) {
                    for (CoreProductAlternativeDomain coreProductAlternativeDomain : productMpnDomains) {
                        if (!indexEditDTO.getMpns().contains(coreProductAlternativeDomain.getMpnAlternative())) {
                            coreProductAlternativeDAO.delete(coreProductAlternativeDomain);
                        }
                    }
                }
                logger.trace("IndexManager:edit:remove old mpns for base product " + TimeUtil.snapshot(start) + " ms.");

                // delete removed TAGs from db
                if (!productTagDomains.isEmpty()) {
                    for (CoreProductTagDomain coreProductTagDomain : productTagDomains) {
                        if (!indexEditDTO.getTags().contains(coreProductTagDomain.getValue())) {
                            coreProductTagDAO.delete(coreProductTagDomain);
                        }
                    }
                }
                logger.trace("IndexManager:edit:remove old tags for base product " + TimeUtil.snapshot(start) + " ms.");

                // add new EANs to db
                for (String ean : indexEditDTO.getEans()) {
                    boolean add = true;
                    for (CoreProductEanDomain coreProductEanDomain : productEanDomains) {
                        if (ean.equals(coreProductEanDomain.getEan())) {
                            add = false;
                        }
                    }
                    if (add) {
                        CoreProductEanDomain coreProductEanDomain = new CoreProductEanDomain();
                        coreProductEanDomain.setCoreProductRepository(coreProductRepositoryDomain);
                        coreProductEanDomain.setEan(ean);
                        coreProductEanDomain.setDateIns(new Date());
                        coreProductEanDomain.setDateUpdate(new Date());
                        coreProductEanDomain.setUserIns(userDAO.getUserByUserKey(userKey).getId());
                        coreProductEanDAO.saveOrUpdate(coreProductEanDomain);
                    }
                }
                logger.trace("IndexManager:edit:add new eans for base product " + TimeUtil.snapshot(start) + " ms.");

                // add new MPNs to db
                for (String mpn : indexEditDTO.getMpns()) {
                    boolean add = true;
                    for (CoreProductAlternativeDomain coreProductAlternativeDomain : productMpnDomains) {
                        if (mpn.equals(coreProductAlternativeDomain.getMpnAlternative())) {
                            add = false;
                        }
                    }
                    if (add) {
                        CoreProductAlternativeDomain coreProductAlternativeDomain = new CoreProductAlternativeDomain();
                        coreProductAlternativeDomain.setCoreProductRepository(coreProductRepositoryDomain);
                        coreProductAlternativeDomain.setMpnAlternative(mpn);
                        coreProductAlternativeDomain.setDateIns(new Date());
                        coreProductAlternativeDomain.setDateUpdate(new Date());
                        coreProductAlternativeDomain.setUserIns(userDAO.getUserByUserKey(userKey).getId());
                        coreProductAlternativeDAO.saveOrUpdate(coreProductAlternativeDomain);
                    }
                }
                logger.trace("IndexManager:edit:add new mpns for base product " + TimeUtil.snapshot(start) + " ms.");

                // add new TAGs to db
                for (String tag : indexEditDTO.getTags()) {
                    boolean add = true;
                    for (CoreProductTagDomain coreProductTagDomain : productTagDomains) {
                        if (tag.equals(coreProductTagDomain.getValue())) {
                            add = false;
                        }
                    }
                    if (add) {
                        CoreProductTagDomain coreProductTagDomain = new CoreProductTagDomain();
                        coreProductTagDomain.setCoreProductRepository(coreProductRepositoryDomain);
                        coreProductTagDomain.setValue(tag);
                        coreProductTagDomain.setDateIns(new Date());
                        coreProductTagDomain.setUserIns(userDAO.getUserByUserKey(userKey).getId());
                        coreProductTagDAO.saveOrUpdate(coreProductTagDomain);
                    }
                }
                logger.trace("IndexManager:edit:add new tags for base product " + TimeUtil.snapshot(start) + " ms.");

                // update core product
                coreProductRepositoryDomain.setMpn(indexEditDTO.getPrimaryMPN());
                coreProductRepositoryDomain.setCategory((newCategoryDomain != null) ? newCategoryDomain : oldCategoryDomain);
                coreProductRepositoryDAO.update(coreProductRepositoryDomain);
                logger.trace("IndexManager:edit:update primary mpn for base product " + TimeUtil.snapshot(start) + " ms.");
            }

            IndexProductDTO indexProductDTO = indexProductDAO.getIndexProduct(coreProductRepositoryDomain.getId());
            if (indexProductDTO != null) {
                indexProductDTO.setCategoryId(coreProductRepositoryDomain.getCategory().getId());
                indexProductDTO.setCategoryName(coreProductRepositoryDomain.getCategory().getName());
                indexProductDTO.setMpnAlternative(indexEditDTO.getMpns());
                indexProductDTO.setEan(indexEditDTO.getEans());
                indexProductDTO.setTag(indexEditDTO.getTags());
                indexProductDTO.setMpn(indexEditDTO.getPrimaryMPN());
                indexProductDAO.update(indexProductDTO);
            }

            //  save statistic staging map
            StatIndexStagingMapDomain statIndexStagingMergeDomain = new StatIndexStagingMapDomain();
            statIndexStagingMergeDomain.setVendorId(coreProductRepositoryDomain.getVendor().getId());
            statIndexStagingMergeDomain.setVendorName(coreProductRepositoryDomain.getVendor().getName());
            statIndexStagingMergeDomain.setCategoryId(coreProductRepositoryDomain.getCategory().getId());
            statIndexStagingMergeDomain.setCategoryName(coreProductRepositoryDomain.getCategory().getName());
            statIndexStagingMergeDomain.setMpn(coreProductRepositoryDomain.getMpn());
            statIndexStagingMergeDomain.setDescription(coreProductRepositoryDomain.getModelName());
            statIndexStagingMergeDomain.setCoreProductRepositoryId(coreProductRepositoryDomain.getId());
            statIndexStagingMergeDomain.setUserInsertId(userDAO.getUserByUserKey(userKey).getId());
            statIndexStagingMergeDomain.setUserLogin(userDAO.getUserByUserKey(userKey).getLogin());
            statIndexStagingMergeDomain.setDateIns(new Date());
            statIndexStagingMergeDomain.setTransactionCode(String.valueOf(Math.abs(coreProductRepositoryDomain.hashCode())));
            statIndexStagingMergeDomain.setRefActivityTypeId(RefActivityTypeEnum.ALTERNATIVE_PRODUCT.ordinal());
            statIndexStagingMapDAO.saveOrUpdate(statIndexStagingMergeDomain);

            // edit category
            if (newCategoryDomain != null && oldCategoryDomain != null) {
                if (!newCategoryDomain.getId().equals(oldCategoryDomain.getId())) {
                    Integer transactionCode = Math.abs(newCategoryDomain.hashCode());
                    List<StagingProductDTO> stagingProductDTOs = stagingProductDAO.getListByCoreProductRepositoryId(coreProductRepositoryDomain.getId());
                    if (!stagingProductDTOs.isEmpty()) {
                        for (StagingProductDTO stagingProductDTO : stagingProductDTOs) {
                            StatIndexStagingMapDomain statIndexStagingMapDomain = new StatIndexStagingMapDomain();
                            statIndexStagingMapDomain.setRefCountryId(stagingProductDTO.getRefCountryId());
                            statIndexStagingMapDomain.setRefCountryName(stagingProductDTO.getCountryName());
                            statIndexStagingMapDomain.setMerchantId(stagingProductDTO.getMerchantId());
                            statIndexStagingMapDomain.setMerchantName(stagingProductDTO.getMerchantName());
                            statIndexStagingMapDomain.setVendorId(stagingProductDTO.getVendorId());
                            statIndexStagingMapDomain.setVendorName(stagingProductDTO.getVendorName());
                            statIndexStagingMapDomain.setCategoryId(stagingProductDTO.getCategoryId());
                            statIndexStagingMapDomain.setCategoryName(stagingProductDTO.getCategoryName());
                            statIndexStagingMapDomain.setMpn(stagingProductDTO.getMpn());
                            statIndexStagingMapDomain.setOrgCategoryName(stagingProductDTO.getOrgCategoryName());
                            statIndexStagingMapDomain.setOrgVendorName(stagingProductDTO.getOrgVendorName());
                            statIndexStagingMapDomain.setOrgMpn(stagingProductDTO.getOrgMpn());
                            statIndexStagingMapDomain.setOrgDeeplink(stagingProductDTO.getDeepLink());
                            statIndexStagingMapDomain.setOrgDescription(stagingProductDTO.getOrgForeingDescription());
                            statIndexStagingMapDomain.setUserInsertId(userDAO.getUserByUserKey(userKey).getId());
                            statIndexStagingMapDomain.setUserLogin(userDAO.getUserByUserKey(userKey).getLogin());
                            statIndexStagingMapDomain.setDateIns(new Date());
                            statIndexStagingMapDomain.setTransactionCode(String.valueOf(transactionCode));
                            statIndexStagingMapDomain.setRefActivityTypeId(RefActivityTypeEnum.CATEGORY_MAPPING.ordinal());
                            statIndexStagingMapDAO.saveOrUpdate(statIndexStagingMapDomain);
                        }
                        // Transform and add into master 
                        // TODO: redevelop to use EJB. 
                        FeedImportManager.getInctance().transformStagingProducts(stagingProductDTOs, transactionCode);
                    }
                }
            }

            logger.trace("IndexManager:edit:update solr index product " + TimeUtil.snapshot(start) + " ms.");
            result = Boolean.TRUE;
        } catch (Exception ex) {
            logger.debug("IndexManager:edit: ", ex);
            logger.error("IndexManager:edit: " + ex.getMessage());
        }
        logger.trace("IndexManager:edit:out time execute " + TimeUtil.snapshot(start) + " ms.");
        return result;
    }

    @Override
    public Boolean validateMPN(IndexEditDTO indexEditDTO) {
        Long start = TimeUtil.start();
        Map sessionFactories = DAOSessionUtil.getSessionFactories();
        AbstractIceleadsSolrDAOFactory solrFactory = DAOFactory.getDAOFactory(sessionFactories).getIceleadsSolrDAOFactory();
        ISolrIndexProductDAO indexProductDAO = solrFactory.getSolrIndexProductDAO();
        boolean success = false;
        try {
            success = indexProductDAO.getCoreProductByMPN(ClientUtils.escapeQueryChars(indexEditDTO.getVendorName()), ClientUtils.escapeQueryChars(indexEditDTO.getMpns().get(0))).isEmpty();
        } catch (Exception ex) {
            logger.debug("IndexManager:validateMPN: ", ex);
            logger.error("IndexManager:validateMPN: " + ex.getMessage());
        }
        logger.trace("IndexManager:validateMPN :out time execute " + TimeUtil.snapshot(start) + " ms.");
        return success;
    }

    @Override
    public Boolean validateEAN(IndexEditDTO indexEditDTO) {
        Long start = TimeUtil.start();
        Map sessionFactories = DAOSessionUtil.getSessionFactories();
        AbstractIceleadsSolrDAOFactory solrFactory = DAOFactory.getDAOFactory(sessionFactories).getIceleadsSolrDAOFactory();
        ISolrIndexProductDAO indexProductDAO = solrFactory.getSolrIndexProductDAO();
        boolean success = false;
        try {
            success = indexProductDAO.getCoreProductByEAN(ClientUtils.escapeQueryChars(indexEditDTO.getEans().get(0))).isEmpty();
        } catch (Exception ex) {
            logger.debug("IndexManager:validateEAN: ", ex);
            logger.error("IndexManager:validateEAN: " + ex.getMessage());
        }
        logger.trace("IndexManager:validateEAN :out time execute " + TimeUtil.snapshot(start) + " ms.");
        return success;
    }

    @Override
    public IndexResultDTO getSolrCoreProduct(IndexEditDTO indexEditDTO) {
        Long start = TimeUtil.start();
        Map sessionFactories = DAOSessionUtil.getSessionFactories();
        AbstractIceleadsSolrDAOFactory solrFactory = DAOFactory.getDAOFactory(sessionFactories).getIceleadsSolrDAOFactory();
        AbstractIceleadsDAOFactory hibernateFactory = DAOFactory.getDAOFactory(sessionFactories).getIceleadsDAOFactory();

        ISolrIndexProductDAO indexProductDAO = solrFactory.getSolrIndexProductDAO();
        ICoreProductRepositoryDAO coreProductRepositoryDAO = hibernateFactory.getCoreProductRepositoryDAO();
        ICoreProductAlternativeDAO coreProductAlternativeDAO = hibernateFactory.getCoreProductAlternativeDAO();
        IVendorDAO vendorDAO = hibernateFactory.getVendorDAO();
        IndexResultDTO resultDTO = new IndexResultDTO();
        
        // duplicate list of  alternative mpns, eans, tags
        List<IndexAlternativeItemDTO> mpns = new ArrayList<IndexAlternativeItemDTO>();
        List<IndexAlternativeItemDTO> eans = new ArrayList<IndexAlternativeItemDTO>();
        List<IndexAlternativeItemDTO> tags = new ArrayList<IndexAlternativeItemDTO>();

        VendorDomain vendorDomain = null;
        CoreProductRepositoryDomain coreProductRepositoryDomain = null;
        List<IndexProductDTO> indexProductDTOs = null;
        List<CoreProductAlternativeDomain> coreProductAlternativeDomains = null;

        boolean success = false;
        try {
            indexProductDTOs = indexProductDAO.getCoreProductByMPN(ClientUtils.escapeQueryChars(indexEditDTO.getVendorName()), ClientUtils.escapeQueryChars(indexEditDTO.getMpns().get(0)));
            
            vendorDomain = vendorDAO.getByName(indexEditDTO.getVendorName());
            if (!(vendorDomain == null && indexEditDTO.getMpns().get(0) == null)) {
                coreProductRepositoryDomain = coreProductRepositoryDAO.getCoreProduct(indexEditDTO.getMpns().get(0), vendorDomain);
                coreProductAlternativeDomains = coreProductAlternativeDAO.getCoreProductAlternative(coreProductRepositoryDomain);
            }
              if (!(indexProductDTOs.isEmpty() && indexProductDTOs == null && coreProductAlternativeDomains == null)) {
                IndexProductDTO indexProdDTO = indexProductDTOs.get(0);
                resultDTO.setProductId(indexProdDTO.getId());
                resultDTO.setMpn(indexProdDTO.getMpn());
                resultDTO.setVendorName(indexProdDTO.getVendorName());
                resultDTO.setVendorId(indexProdDTO.getVendorId());

                for (CoreProductAlternativeDomain  alternativeDomain : coreProductAlternativeDomains) {
                    mpns.add(new IndexAlternativeItemDTO(new Date(), alternativeDomain.getMpnAlternative()));
                }
                for (String e : indexProdDTO.getEan()) {
                    eans.add(new IndexAlternativeItemDTO(new Date(), e));
                }

                for (String t : indexProdDTO.getTag()) {
                    tags.add(new IndexAlternativeItemDTO(new Date(), t));
                }

                resultDTO.setMpns(mpns);
                resultDTO.setEans(eans);
                resultDTO.setTags(tags);
            }

        } catch (Exception ex) {
            logger.debug("IndexManager:validateEAN: ", ex);
            logger.error("IndexManager:validateEAN: " + ex.getMessage());
        }
        logger.trace("IndexManager:validateEAN :out time execute " + TimeUtil.snapshot(start) + " ms.");
        return resultDTO;

    }

    @Override
    public IndexResultDTO initEditSkuFromDB(IndexResultDTO indexResultDTO) {
        Long start = TimeUtil.start();
        Map sessionFactories = DAOSessionUtil.getSessionFactories();
        AbstractIceleadsDAOFactory hibernateFactory = DAOFactory.getDAOFactory(sessionFactories).getIceleadsDAOFactory();
        ICoreProductRepositoryDAO coreProductRepositoryDAO = hibernateFactory.getCoreProductRepositoryDAO();
        ICoreProductEanDAO coreProductEanDAO = hibernateFactory.getCoreProductEanDAO();
        ICoreProductAlternativeDAO coreProductAlternativeDAO = hibernateFactory.getCoreProductAlternativeDAO();
        ICoreProductTagDAO coreProductTagDAO = hibernateFactory.getCoreProductTagDAO();
        CoreProductRepositoryDomain coreProductRepositoryDomain = coreProductRepositoryDAO.getByID(indexResultDTO.getProductId());
        if (coreProductRepositoryDomain != null) {
            List<CoreProductEanDomain> productEanDomains = coreProductEanDAO.getCoreProductEan(coreProductRepositoryDomain);
            List<CoreProductAlternativeDomain> productMpnDomains = coreProductAlternativeDAO.getCoreProductAlternative(coreProductRepositoryDomain);
            List<CoreProductTagDomain> productTagDomains = coreProductTagDAO.getCoreProductTagList(coreProductRepositoryDomain);
            IndexConverter.reinitIndexResultDTO(indexResultDTO, coreProductRepositoryDomain, productMpnDomains, productEanDomains, productTagDomains);
        }
        logger.trace("IndexManager:initEditSkuFromDB :out time execute " + TimeUtil.snapshot(start) + " ms.");
        return indexResultDTO;
    }

    @Override
    public Boolean editCategory(IndexNativeResultDTO indexNativeResultDTO, String newCategoryName, UserDTO userDTO) {
        Long start = TimeUtil.start();
        Map sessionFactories = DAOSessionUtil.getSessionFactories();

        AbstractIceleadsDAOFactory hibernateFactory = DAOFactory.getDAOFactory(sessionFactories).getIceleadsDAOFactory();
        ICoreProductRepositoryDAO coreProductRepositoryDAO = hibernateFactory.getCoreProductRepositoryDAO();
        ICategoryDAO categoryDAO = hibernateFactory.getCategoryDAO();

        AbstractIceleadsSolrDAOFactory solrFactory = DAOFactory.getDAOFactory(sessionFactories).getIceleadsSolrDAOFactory();
        ISolrIndexProductDAO indexProductDAO = solrFactory.getSolrIndexProductDAO();

        AbstractIceleadsSolrDAOFactory stagingSolrFactory = DAOFactory.getDAOFactory(sessionFactories).getStagingSolrDAOFactory();
        ISolrStagingProductDAO stagingProductDAO = stagingSolrFactory.getSolrStagingProductDAO();

        AbstractIceleadsStatDAOFactory hibernateStatFactory = DAOFactory.getDAOFactory(sessionFactories).getIceleadsStatDAOFactory();
        IStatIndexStagingMapDAO statIndexStagingMapDAO = hibernateStatFactory.getStatIndexStagingMapDAO();

        Boolean result = Boolean.FALSE;
        try {
            CoreProductRepositoryDomain coreProductRepositoryDomain = coreProductRepositoryDAO.getByID(indexNativeResultDTO.getCoreProductRepositoryId());
            CategoryDomain categoryDomain = categoryDAO.getCategory(newCategoryName);
            if (coreProductRepositoryDomain != null && categoryDomain != null) {
                coreProductRepositoryDomain.setCategory(categoryDomain);
                coreProductRepositoryDAO.update(coreProductRepositoryDomain);
                IndexProductDTO indexProductDTO = indexProductDAO.getIndexProduct(coreProductRepositoryDomain.getId());
                if (indexProductDTO != null) {
                    indexProductDTO.setCategoryId(coreProductRepositoryDomain.getCategory().getId());
                    indexProductDTO.setCategoryName(coreProductRepositoryDomain.getCategory().getName());
                    indexProductDAO.update(indexProductDTO);
                }

                Integer transactionCode = Math.abs(categoryDomain.hashCode());

                List<StagingProductDTO> stagingProductDTOs = stagingProductDAO.getListByCoreProductRepositoryId(coreProductRepositoryDomain.getId());
                if (!stagingProductDTOs.isEmpty()) {
                    for (StagingProductDTO stagingProductDTO : stagingProductDTOs) {
                        StatIndexStagingMapDomain statIndexStagingMergeDomain = new StatIndexStagingMapDomain();
                        statIndexStagingMergeDomain.setRefCountryId(stagingProductDTO.getRefCountryId());
                        statIndexStagingMergeDomain.setRefCountryName(stagingProductDTO.getCountryName());
                        statIndexStagingMergeDomain.setMerchantId(stagingProductDTO.getMerchantId());
                        statIndexStagingMergeDomain.setMerchantName(stagingProductDTO.getMerchantName());
                        statIndexStagingMergeDomain.setVendorId(stagingProductDTO.getVendorId());
                        statIndexStagingMergeDomain.setVendorName(stagingProductDTO.getVendorName());
                        statIndexStagingMergeDomain.setCategoryId(stagingProductDTO.getCategoryId());
                        statIndexStagingMergeDomain.setCategoryName(stagingProductDTO.getCategoryName());
                        statIndexStagingMergeDomain.setMpn(stagingProductDTO.getMpn());
                        statIndexStagingMergeDomain.setOrgCategoryName(stagingProductDTO.getOrgCategoryName());
                        statIndexStagingMergeDomain.setOrgVendorName(stagingProductDTO.getOrgVendorName());
                        statIndexStagingMergeDomain.setOrgMpn(stagingProductDTO.getOrgMpn());
                        statIndexStagingMergeDomain.setOrgDeeplink(stagingProductDTO.getDeepLink());
                        statIndexStagingMergeDomain.setOrgDescription(stagingProductDTO.getOrgForeingDescription());
                        statIndexStagingMergeDomain.setUserInsertId((userDTO != null) ? userDTO.getUserId() : null);
                        statIndexStagingMergeDomain.setUserLogin((userDTO != null) ? userDTO.getLogin() : null);
                        statIndexStagingMergeDomain.setDateIns(new Date());
                        statIndexStagingMergeDomain.setTransactionCode(String.valueOf(transactionCode));
                        statIndexStagingMergeDomain.setRefActivityTypeId(RefActivityTypeEnum.CATEGORY_MAPPING.ordinal());
                        statIndexStagingMapDAO.saveOrUpdate(statIndexStagingMergeDomain);
                    }
                    // Transform and add into master 
                    // TODO: redevelop to use EJB. 
                    FeedImportManager.getInctance().transformStagingProducts(stagingProductDTOs, transactionCode);
                }
            }
            result = Boolean.TRUE;
        } catch (Exception ex) {
            logger.debug("IndexManager:editCategory: ", ex);
            logger.error("IndexManager:editCategory: " + ex.getMessage());
        }
        logger.trace("IndexManager:editCategory:out time execute " + TimeUtil.snapshot(start) + " ms.");
        return result;
    }
}
