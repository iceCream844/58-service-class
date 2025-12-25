package com.atf.offshore.vka.commissionCalculation.service;

import com.atf.offshore.micro.common.exception.ServiceAppException;
import com.atf.offshore.micro.common.service.BaseOssFileConverter;
import com.atf.offshore.micro.common.service.OssTemplateService;
import com.atf.offshore.vka.commissionCalculation.constant.SftpPathConstant;
import com.atf.offshore.vka.commissionCalculation.dao.AffiliateDAO;
import com.atf.offshore.vka.commissionCalculation.dao.CommissionCP58DAO;
import com.atf.offshore.vka.commissionCalculation.dao.CommissionOCBCGiroDAO;
import com.atf.offshore.vka.commissionCalculation.dao.CompanyDAO;
import com.atf.offshore.vka.commissionCalculation.dao.ConsumeOCBCGiroFileDAO;
import com.atf.offshore.vka.commissionCalculation.dao.DistributeCP58FileDAO;
import com.atf.offshore.vka.commissionCalculation.dao.PlannerDAO;
import com.atf.offshore.vka.commissionCalculation.entity.Affiliate;
import com.atf.offshore.vka.commissionCalculation.entity.CommissionCP58;
import com.atf.offshore.vka.commissionCalculation.entity.CommissionOCBCGiro;
import com.atf.offshore.vka.commissionCalculation.entity.Company;
import com.atf.offshore.vka.commissionCalculation.entity.ConsumeOCBCGiroFile;
import com.atf.offshore.vka.commissionCalculation.entity.DistributeCP58File;
import com.atf.offshore.vka.commissionCalculation.entity.Planner;
import com.atf.offshore.vka.commissionCalculation.enums.ComcalError;
import com.atf.offshore.vka.commissionCalculation.enums.CommissionCP58JobType;
import com.atf.offshore.vka.commissionCalculation.enums.CommissionReceiverType;
import com.atf.offshore.vka.commissionCalculation.enums.JobStatus;
import com.atf.offshore.vka.commissionCalculation.extraction.BaseSFTP;
import com.atf.offshore.vka.commissionCalculation.vo.GenericResponseVo;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import lombok.extern.slf4j.Slf4j;
import net.sf.jasperreports.engine.JREmptyDataSource;
import net.sf.jasperreports.engine.JRException;
import net.sf.jasperreports.engine.JasperCompileManager;
import net.sf.jasperreports.engine.JasperExportManager;
import net.sf.jasperreports.engine.JasperFillManager;
import net.sf.jasperreports.engine.JasperPrint;
import net.sf.jasperreports.engine.JasperReport;
import net.sf.jasperreports.engine.export.ooxml.JRXlsxExporter;
import net.sf.jasperreports.export.SimpleExporterInput;
import net.sf.jasperreports.export.SimpleOutputStreamExporterOutput;
import net.sf.jasperreports.export.SimpleXlsxReportConfiguration;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.*;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.LocalDate;
import java.time.Year;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Service
public class CP58Service extends BaseOssFileConverter {

    private final CommissionOCBCGiroDAO commissionOCBCGiroDAO;
    private final PlannerDAO plannerDAO;
    private final AffiliateDAO affiliateDAO;
    private final CompanyDAO companyDAO;
    private static final String TEMPLATE_NAME = "CP58.jrxml";
    public static final String MALAYSIA = "MALAYSIA";
    private final CommissionCP58JobHelper commissionCP58JobHelper;
    private final BaseSFTP baseSFTP;
    private final CommissionCP58DAO commissionCP58DAO;
    private final ConsumeOCBCGiroFileDAO consumeOCBCGiroFileDAO;
    private final DistributeCP58FileDAO distributeCP58FileDAO;
    private final NotificationService notificationService;

    public CP58Service(CommissionOCBCGiroDAO commissionOCBCGiroDAO,
                       PlannerDAO plannerDAO, CommissionCP58DAO commissionCP58DAO, AffiliateDAO affiliateDAO,
                       CompanyDAO companyDAO, ConsumeOCBCGiroFileDAO consumeOCBCGiroFileDAO, BaseSFTP baseSFTP,
                       OssTemplateService ossTemplateService, CommissionCP58JobHelper commissionCP58JobHelper, DistributeCP58FileDAO distributeCP58FileDAO,
                       NotificationService notificationService) {
        this.commissionOCBCGiroDAO = commissionOCBCGiroDAO;
        this.plannerDAO = plannerDAO;
        this.commissionCP58DAO = commissionCP58DAO;
        this.affiliateDAO = affiliateDAO;
        this.companyDAO = companyDAO;
        this.consumeOCBCGiroFileDAO = consumeOCBCGiroFileDAO;
        this.baseSFTP = baseSFTP;
        this.commissionCP58JobHelper = commissionCP58JobHelper;
        this.distributeCP58FileDAO = distributeCP58FileDAO;
        this.notificationService = notificationService;
        this.ossTemplateService = ossTemplateService;
    }

    @Transactional(rollbackFor = Exception.class)
    public GenericResponseVo generateCP58() {
        GenericResponseVo genericResponseVo = new GenericResponseVo();
        try {
            String templatePath = buildTemplatePath(TEMPLATE_NAME);

            processGiroData();

            List<Map<String, Object>> parametersList = buildReportParameters();

            for (Map<String, Object> parameters : parametersList) {
                String type = parameters.get("type").toString();
                String outputExcelFilePath = null;
                String outputPdfFilePath = null;

                switch (type) {
                    case "ADVISER":
                        outputExcelFilePath = SftpPathConstant.CP58_IN_EXCEL_ADVISERS_PATH + "/";
                        outputPdfFilePath = SftpPathConstant.CP58_IN_PDF_ADVISERS_PATH + "/";
                        break;
                    case "BDD":
                        outputExcelFilePath = SftpPathConstant.CP58_IN_EXCEL_BDD_PATH + "/";
                        outputPdfFilePath = SftpPathConstant.CP58_IN_PDF_BDD_PATH + "/";
                        break;
                    case "BDM":
                        outputExcelFilePath = SftpPathConstant.CP58_IN_EXCEL_BDM_PATH + "/";
                        outputPdfFilePath = SftpPathConstant.CP58_IN_PDF_BDM_PATH + "/";
                        break;
                    case "VEP":
                        outputExcelFilePath = SftpPathConstant.CP58_IN_EXCEL_VEP_PATH + "/";
                        outputPdfFilePath = SftpPathConstant.CP58_IN_PDF_VEP_PATH + "/";
                        break;
                    case "FOV":
                        outputExcelFilePath = SftpPathConstant.CP58_IN_EXCEL_FOV_PATH + "/";
                        outputPdfFilePath = SftpPathConstant.CP58_IN_PDF_FOV_PATH + "/";
                        break;
                    case "MR":
                        outputExcelFilePath = SftpPathConstant.CP58_IN_EXCEL_MR_PATH + "/";
                        outputPdfFilePath = SftpPathConstant.CP58_IN_PDF_MR_PATH + "/";
                        break;
                }

                String fileName = getDynamicReportFileName(parameters);
                generateReport(templatePath, parameters, outputExcelFilePath + fileName, "pdf");
                generateReport(templatePath, parameters, outputPdfFilePath + fileName, "excel");
            }
            commissionCP58JobHelper.updateCP58Job(
                    1L, CommissionCP58JobType.COMMISSION_CP58_JOB_TYPE_1,
                    JobStatus.COMPLETE, false);
            genericResponseVo.setSuccess(true);

            return genericResponseVo;

        } catch (IOException e) {
            String errorMessage = String.format(ComcalError.CP58_PATH_TEMPLATE_ERROR.getDescription(),
                    "CP58 Service", e);
            log.error(errorMessage);
            genericResponseVo.setSuccess(false);
            genericResponseVo.setCode(ComcalError.CP58_PATH_TEMPLATE_ERROR.getCode());
            genericResponseVo.setMessage(errorMessage);
            // Update cp58 job table
            commissionCP58JobHelper.updateCP58Job(
                    1L, CommissionCP58JobType.COMMISSION_CP58_JOB_TYPE_1,
                    JobStatus.FAILED, false);
        } catch (JRException e) {
            String errorMessage = String.format(ComcalError.CP58_JASPER_REPORT_EXPORT_ERROR.getDescription(),
                    "CP58 Service", e);
            log.error(errorMessage);
            genericResponseVo.setSuccess(false);
            genericResponseVo.setCode(ComcalError.CP58_JASPER_REPORT_EXPORT_ERROR.getCode());
            genericResponseVo.setMessage(errorMessage);
            commissionCP58JobHelper.updateCP58Job(
                    1L, CommissionCP58JobType.COMMISSION_CP58_JOB_TYPE_1,
                    JobStatus.FAILED, false);
        } catch (Exception e) {
            String errorMessage = String.format(ComcalError.CP58_GENERAL_ERROR.getDescription(),
                    "CP58 Service", e);
            log.error(errorMessage);
            genericResponseVo.setSuccess(false);
            genericResponseVo.setCode(ComcalError.CALCULATE_GENERAL_EXCEPTION.getCode());
            genericResponseVo.setMessage(errorMessage);
            commissionCP58JobHelper.updateCP58Job(
                    1L, CommissionCP58JobType.COMMISSION_CP58_JOB_TYPE_1,
                    JobStatus.FAILED, false);
        }
        if (!genericResponseVo.getSuccess() || genericResponseVo.getCode() != null) {
            throw new ServiceAppException(HttpStatus.BAD_REQUEST, genericResponseVo.getCode(), genericResponseVo.getMessage());
        } else {
            return genericResponseVo;
        }
    }

    @Transactional(rollbackFor = Exception.class)
    public GenericResponseVo distributeCP58() {
        GenericResponseVo genericResponseVo = new GenericResponseVo();

        String cp58PatternString = "\\w+_[A-Za-z0-9]+_\\d{4}(0[1-9]|1[0-2])(0[1-9]|[12]\\d|3[01])\\.pdf";
        Pattern cp58FilePattern = Pattern.compile(cp58PatternString);

        String cp58OutDirectory = baseSFTP.Cp58Directory() + "OUT/PDF/";
        String distributedCP58Directory = baseSFTP.distributedCP58Directory();
        String archivedCP58Directory = baseSFTP.archivedCP58Directory();

        try {

            distributeFiles(cp58OutDirectory,
                    distributedCP58Directory,
                    cp58FilePattern,
                    CommissionReceiverType.ADVISER);

            distributeFiles(cp58OutDirectory,
                    distributedCP58Directory,
                    cp58FilePattern,
                    CommissionReceiverType.BDD);

            distributeFiles(cp58OutDirectory,
                    distributedCP58Directory,
                    cp58FilePattern,
                    CommissionReceiverType.BDM);

            //Archive the cp58
            archiveCP58(cp58OutDirectory,
                    archivedCP58Directory,
                    cp58FilePattern,
                    CommissionReceiverType.ADVISER);

            archiveCP58(cp58OutDirectory,
                    archivedCP58Directory,
                    cp58FilePattern,
                    CommissionReceiverType.BDD);

            archiveCP58(cp58OutDirectory,
                    archivedCP58Directory,
                    cp58FilePattern,
                    CommissionReceiverType.BDM);

            commissionCP58JobHelper.updateCP58Job(
                    2L, CommissionCP58JobType.COMMISSION_CP58_JOB_TYPE_2,
                    JobStatus.COMPLETE, false);
            genericResponseVo.setSuccess(true);

        } catch (IOException e) {
            String errorMessage = String.format(ComcalError.DISTRIBUTE_CP58_NOT_FOUND.getDescription(),
                    "Distribute CP58 Service", e);
            log.error(errorMessage);
            genericResponseVo.setSuccess(false);
            genericResponseVo.setCode(ComcalError.DISTRIBUTE_CP58_NOT_FOUND.getCode());
            genericResponseVo.setMessage(errorMessage);
            commissionCP58JobHelper.updateCP58Job(
                    2L, CommissionCP58JobType.COMMISSION_CP58_JOB_TYPE_2,
                    JobStatus.FAILED, false);
        } catch (Exception e) {
            String errorMessage = String.format(ComcalError.DISTRIBUTE_CP58_GENERAL_ERROR.getDescription(),
                    "Distribute CP58 Service", e);
            log.error(errorMessage);
            genericResponseVo.setSuccess(false);
            genericResponseVo.setCode(ComcalError.DISTRIBUTE_CP58_GENERAL_ERROR.getCode());
            genericResponseVo.setMessage(errorMessage);
            commissionCP58JobHelper.updateCP58Job(
                    2L, CommissionCP58JobType.COMMISSION_CP58_JOB_TYPE_2,
                    JobStatus.FAILED, false);
        }

        return genericResponseVo;
    }

    public static String formatAddress(String jsonAddress) {
        if (jsonAddress != null) {
            String cleanedJson = jsonAddress.replaceAll("[{}\"]", "");
            String[] keyValuePairs = cleanedJson.split(",");
            String addressLine1 = "";
            String addressLine2 = "";
            String city = "";
            String state = "";
            String country = "";
            String postcode = "";
            String extra = "";

            for (String pair : keyValuePairs) {
                String[] keyValue = pair.split(":");
                String key = keyValue[0].trim();
                String value = keyValue.length > 1 ? keyValue[1].trim() : "";

                switch (key) {
                    case "addressLine1":
                        addressLine1 = value;
                        break;
                    case "addressLine2":
                        addressLine2 = value;
                        break;
                    case "city":
                        city = value;
                        break;
                    case "state":
                        state = value;
                        break;
                    case "country":
                        country = value;
                        break;
                    case "postcode":
                        postcode = value;
                        break;
                    default:
                        extra = value;
                }
            }

            StringBuilder formattedAddress = new StringBuilder();
            if (!addressLine1.isEmpty()) {
                formattedAddress.append(addressLine1);
            }
            if (!addressLine2.isEmpty()) {
                if (formattedAddress.length() > 0) formattedAddress.append(", ");
                formattedAddress.append(addressLine2);
            }
            if (!city.isEmpty()) {
                if (formattedAddress.length() > 0) formattedAddress.append(", ");
                formattedAddress.append(city);
            }
            if (!state.isEmpty()) {
                if (formattedAddress.length() > 0) formattedAddress.append(", ");
                formattedAddress.append(state);
            }
            if (!postcode.isEmpty()) {
                if (formattedAddress.length() > 0) formattedAddress.append(", ");
                formattedAddress.append(postcode);
            }
            if (!country.isEmpty()) {
                if (formattedAddress.length() > 0) formattedAddress.append(", ");
                formattedAddress.append(country);
            }
            if (!extra.isEmpty()) {
                if (formattedAddress.length() > 0) formattedAddress.append(", ");
                formattedAddress.append(extra);
            }

            return formattedAddress.toString();
        }
        return "";
    }

    private List<Map<String, Object>> buildReportParameters() {

        int year = Year.now().getValue() - 1;
        List<CommissionCP58> adviserList = commissionCP58DAO.findByRecipientTypeAndYears(CommissionReceiverType.ADVISER, year);
        List<CommissionCP58> bdmList = commissionCP58DAO.findByRecipientTypeAndYears(CommissionReceiverType.BDM, year);
        List<CommissionCP58> bddList = commissionCP58DAO.findByRecipientTypeAndYears(CommissionReceiverType.BDD, year);
        List<CommissionCP58> vepList = commissionCP58DAO.findByRecipientTypeAndYears(CommissionReceiverType.VEP, year);
        List<CommissionCP58> fovList = commissionCP58DAO.findByRecipientTypeAndYears(CommissionReceiverType.FOV, year);
        List<CommissionCP58> mrList = commissionCP58DAO.findByRecipientTypeAndYears(CommissionReceiverType.MR, year);

        List<Map<String, Object>> parameterList = new ArrayList<>();
        parameterList.addAll(buildParametersForList(adviserList, CommissionReceiverType.ADVISER));
        parameterList.addAll(buildParametersForList(bdmList, CommissionReceiverType.BDM));
        parameterList.addAll(buildParametersForList(bddList, CommissionReceiverType.BDD));
        parameterList.addAll(buildParametersForList(vepList, CommissionReceiverType.VEP));
        parameterList.addAll(buildParametersForList(fovList, CommissionReceiverType.FOV));
        parameterList.addAll(buildParametersForList(mrList, CommissionReceiverType.MR));

        return parameterList;
    }

    private String getDynamicReportFileName(Map<String, Object> parameters) {
        String code = (String) parameters.get("code");
        String recipientName = (String) parameters.get("recipientName");
        String sanitizedRecipientName = recipientName.replaceAll("\\s+", "");
        String currentDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));

        String fileName = String.format("%s_%s_%s", code, sanitizedRecipientName, currentDate);

        return fileName;
    }

    private String buildTemplatePath(String templateName) {
        return Paths.get(baseSFTP.cp58Template(), templateName).toString();
    }

    private void processGiroData() {

        int currentYear = Year.now().getValue();
        List<ConsumeOCBCGiroFile> giroFileList = consumeOCBCGiroFileDAO.findByFileYear(currentYear);
        List<ConsumeOCBCGiroFile> adviserGiroFiles = new ArrayList<>();
        List<ConsumeOCBCGiroFile> affiliateGiroFiles = new ArrayList<>();
        List<ConsumeOCBCGiroFile> companyGiroFiles = new ArrayList<>();

        for (ConsumeOCBCGiroFile giroFile : giroFileList) {
            if (giroFile.getAdviser() != null) {
                adviserGiroFiles.add(giroFile);
            } else if (giroFile.getAffiliate() != null) {
                affiliateGiroFiles.add(giroFile);
            } else if (giroFile.getCompany() != null) {
                companyGiroFiles.add(giroFile);
            }
        }
        //process recipient giro to database
        processAdviser(adviserGiroFiles);
        processBdmBdd(companyGiroFiles);
        processVepMrFov(affiliateGiroFiles);

    }

    private List<Map<String, Object>> buildParametersForList(List<CommissionCP58> commissionList, CommissionReceiverType type) {
        List<Map<String, Object>> parameterList = new ArrayList<>();

        for (CommissionCP58 cp58 : commissionList) {
            Map<String, Object> parameters = new HashMap<>();

            parameters.put("identificationId", cp58.getRecipientIdentificationNo() != null ? cp58.getRecipientIdentificationNo() : "");
            parameters.put("commission", BigDecimal.valueOf(0));
            parameters.put("vehicle", cp58.getTotalVehicleIncentive() != null ? cp58.getTotalVehicleIncentive() : BigDecimal.valueOf(0));
            parameters.put("house", cp58.getTotalHouseIncentive() != null ? cp58.getTotalHouseIncentive() : BigDecimal.valueOf(0));
            parameters.put("travel", cp58.getTotalTourTravelPackageIncentive() != null ? cp58.getTotalTourTravelPackageIncentive() : BigDecimal.valueOf(0));
            parameters.put("others1", cp58.getTotalReferralAmount() != null ? cp58.getTotalReferralAmount() : BigDecimal.valueOf(0));
            parameters.put("training", cp58.getTotalTrainingDevelopmentIncentive() != null ? cp58.getTotalTrainingDevelopmentIncentive() : BigDecimal.valueOf(0));
            parameters.put("others2", cp58.getTotalOtherIncentive() != null ? cp58.getTotalOtherIncentive() : BigDecimal.valueOf(0));

            Map<String, String> taxParts = extractTaxNoParts(cp58.getRecipientIncomeTaxNo() != null ? cp58.getRecipientIncomeTaxNo() : "");
            parameters.put("taxNo1", taxParts.get("taxNo1"));
            parameters.put("taxNo2", taxParts.get("taxNo2"));

            parameters.put("residentInMalaysia", cp58.getResidentInMalaysia() != null ? cp58.getResidentInMalaysia() : "");
            parameters.put("recipientName", cp58.getRecipientName() != null ? cp58.getRecipientName() : "");
            parameters.put("recipientAddress", cp58.getRecipientAddress() != null ? cp58.getRecipientAddress() : "");
            parameters.put("year", cp58.getYears() + 1);
            parameters.put("code", cp58.getRecipientCode());
            parameters.put("type", type);

            parameterList.add(parameters);
        }

        return parameterList;
    }

    private void processVepMrFov(List<ConsumeOCBCGiroFile> fovGiroFiles) {

        Map<Long, List<ConsumeOCBCGiroFile>> groupedByFovVepMr = fovGiroFiles.stream()
                .collect(Collectors.groupingBy(giroFile -> giroFile.getAffiliate().getId()));

        for (Map.Entry<Long, List<ConsumeOCBCGiroFile>> entry : groupedByFovVepMr.entrySet()) {
            Long affiliateId = entry.getKey();
            List<ConsumeOCBCGiroFile> giroFilesForFovVepMr = entry.getValue();

            Map<String, BigDecimal> totals = calculateIncentiveTotals(giroFilesForFovVepMr);

            // Fetch Affiliate
            Affiliate affiliate = affiliateDAO.findById(affiliateId)
                    .orElseThrow(() -> new ServiceAppException(HttpStatus.BAD_REQUEST, ComcalError.CP58_AFFILIATE_NOT_FOUND.getCode() + affiliateId));

            Long recipientId = 0L;
            if (affiliate.getFov() != null) {
                recipientId = affiliate.getFov().getId();
            } else if (affiliate.getVep() != null) {
                recipientId = affiliate.getVep().getId();
            } else if (affiliate.getMr() != null) {
                recipientId = affiliate.getMr().getId();
            }

            List<CommissionOCBCGiro> ocbcGiroList = commissionOCBCGiroDAO.findByCommissionReceiverTypeId(recipientId);

            CommissionReceiverType commissionReceiverType = ocbcGiroList.get(0).getCommissionReceiverType();

            CommissionCP58 commissionCP58 = commissionCP58DAO.findByRecipientIdAndRecipientTypeAndYears(recipientId, commissionReceiverType, Year.now().getValue() - 1);
            if (commissionCP58 == null) {
                commissionCP58 = new CommissionCP58();
            }

            switch (commissionReceiverType) {
                case VEP:
                    commissionCP58.setRecipientId(affiliate.getVep().getId());
                    commissionCP58.setRecipientCode(affiliate.getVep().getCode());
                    commissionCP58.setRecipientIdentificationNo(affiliate.getIdentificationNumber());
                    break;
                case FOV:
                    commissionCP58.setRecipientId(affiliate.getFov().getId());
                    commissionCP58.setRecipientCode(affiliate.getFov().getCode());
                    commissionCP58.setRecipientIdentificationNo(affiliate.getIdentificationNumber() != null ? affiliate.getIdentificationNumber() : "");
                    commissionCP58.setBusinessRegistrationNo(affiliate.getFov().getBusinessRegistrationNumber() != null ? affiliate.getIdentificationNumber() : "");
                    break;
                case MR:
                    commissionCP58.setRecipientId(affiliate.getMr().getId());
                    commissionCP58.setRecipientCode(affiliate.getMr().getCode());
                    commissionCP58.setRecipientIdentificationNo(affiliate.getIdentificationNumber());
                    break;
                default:
                    throw new ServiceAppException(HttpStatus.BAD_REQUEST, ComcalError.CP58_INVALID_RECIPIENT_TYPE.getCode() + commissionReceiverType);
            }

            commissionCP58.setRecipientName(affiliate.getName());
            commissionCP58.setRecipientType(commissionReceiverType);
            commissionCP58.setTotalReferralAmount(totals.getOrDefault("totalReferralFee", BigDecimal.ZERO));
            commissionCP58.setTotalHouseIncentive(totals.getOrDefault("totalHouseIncentive", BigDecimal.ZERO));
            commissionCP58.setTotalTourTravelPackageIncentive(totals.getOrDefault("totalTourTravelIncentive", BigDecimal.ZERO));
            commissionCP58.setTotalOtherIncentive(totals.getOrDefault("totalOthers", BigDecimal.ZERO));
            commissionCP58.setTotalVehicleIncentive(totals.getOrDefault("totalVehicleIncentive", BigDecimal.ZERO));
            commissionCP58.setRecipientIncomeTaxNo("");
            commissionCP58.setRecipientAddress(formatAddress(affiliate.getCorrespondingAddress()));
            commissionCP58.setResidentInMalaysia(isAddressInMalaysia(affiliate.getCorrespondingAddress()));
            commissionCP58.setYears(Year.now().getValue() - 1);

            commissionCP58DAO.save(commissionCP58);
        }
    }

    private void processBdmBdd(List<ConsumeOCBCGiroFile> companyGiroFiles) {

        Map<Long, List<ConsumeOCBCGiroFile>> groupedByCompany = companyGiroFiles.stream()
                .collect(Collectors.groupingBy(giroFile -> giroFile.getCompany().getId()));

        for (Map.Entry<Long, List<ConsumeOCBCGiroFile>> entry : groupedByCompany.entrySet()) {
            Long companyId = entry.getKey();
            List<ConsumeOCBCGiroFile> giroFilesForFovVepMr = entry.getValue();

            Map<String, BigDecimal> totals = calculateIncentiveTotals(giroFilesForFovVepMr);

            List<CommissionOCBCGiro> ocbcGiroList = commissionOCBCGiroDAO.findByCommissionReceiverTypeId(companyId);

            CommissionReceiverType commissionReceiverType = ocbcGiroList.get(0).getCommissionReceiverType();

            Company company = companyDAO.findById(companyId)
                    .orElseThrow(() -> new ServiceAppException(HttpStatus.BAD_REQUEST, ComcalError.CP58_AFFILIATE_NOT_FOUND.getCode(), companyId));

            CommissionCP58 commissionCP58 = commissionCP58DAO.findByRecipientIdAndRecipientTypeAndYears(companyId, commissionReceiverType, Year.now().getValue() - 1);
            if (commissionCP58 == null) {
                commissionCP58 = new CommissionCP58();
            }

            commissionCP58.setRecipientId(companyId);
            commissionCP58.setRecipientCode(company.getCode());
            commissionCP58.setRecipientName(company.getName());
            commissionCP58.setRecipientType(commissionReceiverType);
            commissionCP58.setBusinessRegistrationNo(company.getNewBusinessRegNo());
            commissionCP58.setTotalTrainingDevelopmentIncentive(totals.get("totalTrainingIncentive"));
            commissionCP58.setTotalHouseIncentive(totals.get("totalHouseIncentive"));
            commissionCP58.setTotalTourTravelPackageIncentive(totals.get("totalTourTravelIncentive"));
            commissionCP58.setTotalOtherIncentive(totals.get("totalOthers"));
            commissionCP58.setTotalVehicleIncentive(totals.get("totalVehicleIncentive"));
            commissionCP58.setRecipientIncomeTaxNo(company.getCompanyOwner().getIncomeTaxNo());
            commissionCP58.setRecipientAddress(formatAddress(company.getBranch().getAddress()));
            commissionCP58.setResidentInMalaysia(isAddressInMalaysia(company.getBranch().getAddress()));
            commissionCP58.setYears(Year.now().getValue() - 1);

            commissionCP58DAO.save(commissionCP58);
        }
    }

    private Map<String, BigDecimal> calculateIncentiveTotals(List<ConsumeOCBCGiroFile> giroFiles) {
        Map<String, BigDecimal> totals = new HashMap<>();
        totals.put("totalReferralFee", BigDecimal.ZERO);
        totals.put("totalVehicleIncentive", BigDecimal.ZERO);
        totals.put("totalHouseIncentive", BigDecimal.ZERO);
        totals.put("totalTourTravelIncentive", BigDecimal.ZERO);
        totals.put("totalOthers", BigDecimal.ZERO);
        totals.put("totalTrainingIncentive", BigDecimal.ZERO);

        for (ConsumeOCBCGiroFile giroFile : giroFiles) {
            if (giroFile.getTotalReferralFee() != null) {
                totals.put("totalReferralFee", totals.get("totalReferralFee").add(giroFile.getTotalReferralFee()));
            }
            if (giroFile.getTotalVehicleIncentive() != null) {
                totals.put("totalVehicleIncentive", totals.get("totalVehicleIncentive").add(giroFile.getTotalVehicleIncentive()));
            }
            if (giroFile.getTotalHouseIncentive() != null) {
                totals.put("totalHouseIncentive", totals.get("totalHouseIncentive").add(giroFile.getTotalHouseIncentive()));
            }
            if (giroFile.getTotalOthers() != null) {
                totals.put("totalOthers", totals.get("totalOthers").add(giroFile.getTotalOthers()));
            }
            if (giroFile.getTotalTourTravelPackageIncentive() != null) {
                totals.put("totalTourTravelIncentive", totals.get("totalTourTravelIncentive").add(giroFile.getTotalTourTravelPackageIncentive()));
            }
            if (giroFile.getTotalTrainingDevelopmentIncentive() != null) {
                totals.put("totalTrainingIncentive", totals.get("totalTrainingIncentive").add(giroFile.getTotalTrainingDevelopmentIncentive()));
            }
        }
        return totals;
    }

    private void processAdviser(List<ConsumeOCBCGiroFile> adviserGiroFiles) {

        Map<Long, List<ConsumeOCBCGiroFile>> groupedByAdviser = adviserGiroFiles.stream()
                .collect(Collectors.groupingBy(giroFile -> giroFile.getAdviser().getId()));

        // Process each adviser

        for (Map.Entry<Long, List<ConsumeOCBCGiroFile>> entry : groupedByAdviser.entrySet()) {
            Long adviserId = entry.getKey();
            List<ConsumeOCBCGiroFile> giroFilesForAdviser = entry.getValue();

            Map<String, BigDecimal> totals = calculateIncentiveTotals(giroFilesForAdviser);

            List<CommissionOCBCGiro> ocbcGiroList = commissionOCBCGiroDAO.findByCommissionReceiverTypeId(adviserId);

            CommissionReceiverType commissionReceiverType = ocbcGiroList.get(0).getCommissionReceiverType();

            Planner adviser = plannerDAO.findById(adviserId)
                    .orElseThrow(() -> new ServiceAppException(HttpStatus.BAD_REQUEST, ComcalError.CP58_ADVISER_NOT_FOUND.getCode(), adviserId));

            CommissionCP58 commissionCP58 = commissionCP58DAO.findByRecipientIdAndRecipientTypeAndYears(adviserId, commissionReceiverType, Year.now().getValue() - 1);
            if (commissionCP58 == null) {
                commissionCP58 = new CommissionCP58();
            }

            commissionCP58.setRecipientId(adviserId);
            commissionCP58.setRecipientCode(adviser.getVkaCode());
            commissionCP58.setRecipientName(adviser.getPreferredName());
            commissionCP58.setRecipientType(commissionReceiverType);
            commissionCP58.setRecipientIdentificationNo(adviser.getIdentificationNo());
            commissionCP58.setTotalReferralAmount(totals.get("totalReferralFee"));
            commissionCP58.setTotalHouseIncentive(totals.get("totalHouseIncentive"));
            commissionCP58.setTotalTourTravelPackageIncentive(totals.get("totalTourTravelIncentive"));
            commissionCP58.setTotalOtherIncentive(totals.get("totalOthers"));
            commissionCP58.setTotalVehicleIncentive(totals.get("totalVehicleIncentive"));
            commissionCP58.setRecipientIncomeTaxNo(adviser.getIncomeTaxNo());
            commissionCP58.setRecipientAddress(formatAddress(adviser.getResidentialAddress()));
            commissionCP58.setResidentInMalaysia(isAddressInMalaysia(adviser.getResidentialAddress()));
            commissionCP58.setYears(Year.now().getValue() - 1);

            commissionCP58DAO.save(commissionCP58);
        }
    }

    private void generateReport(String reportPath, Map<String, Object> parameters, String outputFilePath, String format) throws IOException, JRException {
        try (InputStream reportStream = Files.newInputStream(Paths.get(reportPath))) {
            JasperReport jasperReport = JasperCompileManager.compileReport(reportStream);
            JREmptyDataSource dataSource = new JREmptyDataSource();
            JasperPrint jasperPrint = JasperFillManager.fillReport(jasperReport, parameters, dataSource);

            // Export report based on the format
            switch (format.toLowerCase()) {
                case "pdf":

                    File tempFile = File.createTempFile("report_", ".pdf");
                    JasperExportManager.exportReportToPdfFile(jasperPrint, tempFile.getAbsolutePath());

                    InputStream inputStream = Files.newInputStream(tempFile.toPath());
                    baseSFTP.uploadFileToSFTP(inputStream, outputFilePath + ".pdf");

                    if (tempFile.exists()) {
                        tempFile.delete();
                    }

                    break;
                case "excel":
                    exportReportToExcel(jasperPrint, outputFilePath + ".xlsx");
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported format: " + format);
            }
        } catch (JSchException | SftpException e) {
            throw new RuntimeException(e);
        }
    }


    public Short isAddressInMalaysia(String jsonAddress) {
        JsonObject addressObject = JsonParser.parseString(jsonAddress).getAsJsonObject();

        String country = addressObject.has("country") ? addressObject.get("country").getAsString() : null;

        if (MALAYSIA.equalsIgnoreCase(country)) {
            return 1;
        }

        return 2;
    }


    private Map<String, String> extractTaxNoParts(String incomeTaxNo) {
        Map<String, String> taxParts = new HashMap<>();
        String taxNo1 = "";
        String taxNo2 = "";

        if (incomeTaxNo.startsWith("C1")) {
            taxNo1 = "C1";
            taxNo2 = incomeTaxNo.substring(2);
        } else {
            StringBuilder letters = new StringBuilder();
            StringBuilder digits = new StringBuilder();

            for (char c : incomeTaxNo.toCharArray()) {
                if (Character.isLetter(c)) {
                    letters.append(c);
                } else if (Character.isDigit(c)) {
                    digits.append(c);
                }
            }

            taxNo1 = letters.toString();
            taxNo2 = digits.toString();
        }

        taxParts.put("taxNo1", taxNo1);
        taxParts.put("taxNo2", taxNo2);

        return taxParts;
    }

    private void exportReportToExcel(JasperPrint jasperPrint, String outputFilePath) throws JRException, JSchException, SftpException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        JRXlsxExporter exporter = new JRXlsxExporter();
        exporter.setExporterInput(new SimpleExporterInput(jasperPrint));
        exporter.setExporterOutput(new SimpleOutputStreamExporterOutput(byteArrayOutputStream));

        SimpleXlsxReportConfiguration configuration = new SimpleXlsxReportConfiguration();
        configuration.setDetectCellType(true);
        configuration.setOnePagePerSheet(false);
        configuration.setRemoveEmptySpaceBetweenRows(true);
        configuration.setWhitePageBackground(false);

        exporter.setConfiguration(configuration);
        exporter.exportReport();

        InputStream inputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        baseSFTP.uploadFileToSFTP(inputStream, outputFilePath);
    }

    private void distributeFiles(String sourceDirPath, String targetDirPath, Pattern pattern, CommissionReceiverType type) throws IOException {
        Path sourceDir = Paths.get(sourceDirPath);
        Path targetDir = Paths.get(targetDirPath);

        switch (type) {
            case ADVISER:
                sourceDir = Paths.get(sourceDirPath + "ADVISER");
                targetDir = Paths.get(targetDir + "/ADVISER");
                break;
            case BDM:
                sourceDir = Paths.get(sourceDirPath + "BDM");
                targetDir = Paths.get(targetDir + "/BDM");
                break;
            case BDD:
                sourceDir = Paths.get(sourceDirPath + "BDD");
                targetDir = Paths.get(targetDir + "/BDD");
                break;
            default:
                throw new ServiceAppException(HttpStatus.BAD_REQUEST, ComcalError.CP58_INVALID_RECIPIENT_TYPE.getCode() + type);
        }
        createDirectoryIfNotExists(sourceDir);
        try (Stream<Path> filesStream = Files.list(sourceDir)) {
            List<String> matchedFiles = findMatchingFiles(filesStream, pattern);

            if (matchedFiles.isEmpty()) {
                log.info("No files found in: " + sourceDir);
            }
            createDirectoryIfNotExists(targetDir);

            for (String fileName : matchedFiles) {
                Path sourceFile = sourceDir.resolve(fileName);
                Path targetFile = targetDir.resolve(fileName);

                copyFile(sourceFile, targetFile);
                saveDistributedCP58(targetFile, type);
            }

            log.info("Processed " + matchedFiles.size() + " files from: " + sourceDir);
        }
    }

    private void archiveCP58(String sourceDirPath, String targetDirPath, Pattern pattern, CommissionReceiverType type) throws IOException {
        Path sourceDir = Paths.get(sourceDirPath);
        Path targetDir = Paths.get(targetDirPath);

        switch (type) {
            case ADVISER:
                sourceDir = Paths.get(sourceDirPath + "ADVISER");
                targetDir = Paths.get(targetDir + "/ADVISER");
                break;
            case BDM:
                sourceDir = Paths.get(sourceDirPath + "BDM");
                targetDir = Paths.get(targetDir + "/BDM");
                break;
            case BDD:
                sourceDir = Paths.get(sourceDirPath + "BDD");
                targetDir = Paths.get(targetDir + "/BDD");
                break;
            default:
                throw new ServiceAppException(HttpStatus.BAD_REQUEST, ComcalError.CP58_INVALID_RECIPIENT_TYPE.getCode() + type);
        }

        try (Stream<Path> filesStream = Files.list(sourceDir)) {
            List<String> matchedFiles = findMatchingFiles(filesStream, pattern);

            if (matchedFiles.isEmpty()) {
                log.info("No files found in: " + sourceDir);
            }
            createDirectoryIfNotExists(targetDir);

            for (String fileName : matchedFiles) {
                Path sourceFile = sourceDir.resolve(fileName);
                Path targetFile = targetDir.resolve(fileName);
                copyFile(sourceFile, targetFile);
                Files.delete(sourceFile);
            }
        }
    }

    private void saveDistributedCP58(Path filePath, CommissionReceiverType type) {
        DistributeCP58File distributeCP58File = new DistributeCP58File();
        String fileName = filePath.getFileName().toString();
        String[] fileNameParts = fileName.split("_");
        String code = fileNameParts[0];

        switch (type) {
            case ADVISER:
                Planner planner = plannerDAO.findByVkaCode(code);
                distributeCP58File.setReceivedType(CommissionReceiverType.ADVISER);
                distributeCP58File.setAdviser(planner);
                //Send Notification to Adviser
                notificationService.sendPaymentVoucherDistributionComplete(planner.getId(), planner);
                break;
            case BDM:
                distributeCP58File.setReceivedType(CommissionReceiverType.BDM);
                distributeCP58File.setCompany(
                        companyDAO.findByCode(code)
                );
                break;
            case BDD:
                distributeCP58File.setReceivedType(CommissionReceiverType.BDD);
                distributeCP58File.setCompany(
                        companyDAO.findByCode(code)
                );
                break;
            default:
                throw new ServiceAppException(HttpStatus.BAD_REQUEST, ComcalError.CP58_INVALID_RECIPIENT_TYPE.getCode() + type);
        }
        distributeCP58File.setName(fileName);
        distributeCP58File.setPath(filePath.toString());
        distributeCP58FileDAO.save(distributeCP58File);

    }

    private void createDirectoryIfNotExists(Path directory) throws IOException {
        if (!Files.exists(directory)) {
            Files.createDirectories(directory);
            log.info("Created directory: " + directory);
        }
    }

    private void copyFile(Path sourceFile, Path targetFile) throws IOException {
        Files.copy(sourceFile, targetFile, StandardCopyOption.REPLACE_EXISTING);
        log.info("File copied from " + sourceFile + " to " + targetFile);
    }

    private List<String> findMatchingFiles(Stream<Path> filesStream, Pattern pattern) {
        return filesStream.map(path -> path.getFileName().toString())
                .filter(fileName -> pattern.matcher(fileName).matches())
                .collect(Collectors.toList());
    }
}
