

/*
DROP TABLE utility_staging.DW_Process_Master;	-- DROP TABLE utility_staging.DW_Column_Config
DROP TABLE utility_staging.DW_Process_Stage_Detail;	-- DROP TABLE utility_staging.DW_Column_Config
DROP TABLE utility_staging.DW_Table_Config;	-- DROP TABLE utility_staging.DW_Column_Config
DROP TABLE utility_staging.DW_Column_Config;	-- DROP TABLE utility_staging.DW_Column_Config
DROP TABLE utility_staging.DW_Output_Table;	-- DROP TABLE utility_staging.DW_Column_Config
DROP TABLE utility_staging.DW_Output_Column_Config;	-- DROP TABLE utility_staging.DW_Column_Config
*/

SELECT * FROM utility_staging.DW_Process_Master;	-- DROP TABLE utility_staging.DW_Column_Config
SELECT * FROM utility_staging.DW_Process_Stage_Detail;	-- DROP TABLE utility_staging.DW_Column_Config
SELECT * FROM utility_staging.DW_Table_Config;	-- DROP TABLE utility_staging.DW_Column_Config
SELECT * FROM utility_staging.DW_Column_Config;	-- DROP TABLE utility_staging.DW_Column_Config
SELECT * FROM utility_staging.DW_Output_Table_Config;	-- DROP TABLE utility_staging.DW_Column_Config
SELECT * FROM utility_staging.DW_Output_Column_Config;	-- DROP TABLE utility_staging.DW_Column_Config

-- Step 1 : Table config for staging
-- INSERT INTO utility_staging.DW_Table_Config(SchemaName,TableName,ActiveFlag,LoadType,RefreshFrequency)
SELECT 'financialForms' AS SchemaName
		, 'guarantee_accounts' AS TableName
		, 1 AS ActiveFlag
		, 'FULL' AS LoadType
		, 'EOD' AS RefreshFrequency,
		, 'Rohan' AS CreatedBy;

-- Step 2 : Table Wise Column config for staging
-- INSERT INTO utility_staging.DW_Column_Config(TableID,ColumnName,AliasName,IncludeFlag,TransformationLogic)
SELECT 1,'id','Deal_ID',1,NULL UNION ALL
SELECT 1,'version','Version',0,NULL UNION ALL
SELECT 1,'mli_code','Partner_Code',1,NULL UNION ALL
SELECT 1,'guarantee_product_code','Deal_Product_Code',1,NULL UNION ALL
SELECT 1,'guarantee_product_name','Deal_Name',1,NULL UNION ALL
SELECT 1,'guarantee_account_number','Deal_Account_Number',1,NULL UNION ALL
SELECT 1,'guarantee_sanction_amount','Deal_Sanction_Amount',1,NULL UNION ALL
SELECT 1,'guarantee_start_date','Deal_Start_Date',1,NULL UNION ALL
SELECT 1,'guarantee_end_date','Deal_End_Date',1,NULL UNION ALL
SELECT 1,'guarantee_start_condition','Deal_Start_Condition',1,NULL UNION ALL
SELECT 1,'fee_code','Fee_Code',0,NULL UNION ALL
SELECT 1,'fee_type','Fee_Type',0,NULL UNION ALL
SELECT 1,'fee_rate','Fee_Rate',0,NULL UNION ALL
SELECT 1,'number_of_pools','Number_Of_Pools',0,NULL UNION ALL
SELECT 1,'type_of_loss_sharing_arrangement','Type_Of_Loss_Sharing_Arrangement',1,NULL UNION ALL
SELECT 1,'loss_calculatin_method','Loss_Calculatin_Method',1,NULL UNION ALL
SELECT 1,'loan_level_coverage','Loan_Level_Coverage',0,NULL UNION ALL
SELECT 1,'pool_level_coverage','Pool_Level_Coverage',0,NULL UNION ALL
SELECT 1,'second_loss_trigger','Second_Loss_Trigger',0,NULL UNION ALL
SELECT 1,'pari_passu_sharing','Pari_Passu_Sharing',0,NULL UNION ALL
SELECT 1,'first_claim_trigger_reference_event','First_Claim_Trigger_Reference_Event',1,NULL UNION ALL
SELECT 1,'first_claim_cooling_period','First_Claim_Cooling_Period',0,NULL UNION ALL
SELECT 1,'first_claim_pay_out_amount','First_Claim_Pay_Out_Amount',0,NULL UNION ALL
SELECT 1,'subsequent_claim_pay_out_frequency','Subsequent_Claim_Pay_Out_Frequency',0,NULL UNION ALL
SELECT 1,'pool_onboarding_method','Pool_Onboarding_Method',0,NULL UNION ALL
SELECT 1,'type_of_pool','Type_Of_Pool',0,NULL UNION ALL
SELECT 1,'guarantee_score_card','Guarantee_Score_Card',1,NULL UNION ALL
SELECT 1,'business_rules','Business_Rules',1,NULL UNION ALL
SELECT 1,'covenants','Covenants',0,NULL UNION ALL
SELECT 1,'pan_check_required','Pan_Check_Required',1,NULL UNION ALL
SELECT 1,'aadhaar_check_required','Aadhaar_Check_Required',1,NULL UNION ALL
SELECT 1,'voter_id_check_required','Voter_Id_Check_Required',1,NULL UNION ALL
SELECT 1,'passport_check_required','Passport_Check_Required',1,NULL UNION ALL
SELECT 1,'driving_license_check_required','Driving_License_Check_Required',1,NULL UNION ALL
SELECT 1,'bank_account_validation_required','Bank_Account_Validation_Required',1,NULL UNION ALL
SELECT 1,'application_date','Application_Date',1,NULL UNION ALL
SELECT 1,'relationship_manager','Relationship_Manager',1,NULL UNION ALL
SELECT 1,'approval_date','Approval_Date',1,NULL UNION ALL
SELECT 1,'approved_by_1','Approved_By_1',1,NULL UNION ALL
SELECT 1,'approved_by_2','Approved_By_2',1,NULL UNION ALL
SELECT 1,'approved_by_3','Approved_By_3',1,NULL UNION ALL
SELECT 1,'late_fee_grace_period','Late_Fee_Grace_Period',0,NULL UNION ALL
SELECT 1,'created_by','Created_By',1,NULL UNION ALL
SELECT 1,'created_at','Created_At',1,NULL UNION ALL
SELECT 1,'last_edited_by','Last_Edited_By',1,NULL UNION ALL
SELECT 1,'last_edited_at','Last_Edited_At',1,NULL UNION ALL
SELECT 1,'is_active','Is_Active',1,NULL UNION ALL
SELECT 1,'guarantee_type','Guarantee_Type',1,NULL UNION ALL
SELECT 1,'lender_processing_fee','Lender_Processing_Fee',1,NULL UNION ALL
SELECT 1,'originator_servicer_fee','Originator_Servicer_Fee',1,NULL UNION ALL
SELECT 1,'collection_splitting_methodology','Collection_Splitting_Methodology',1,NULL UNION ALL
SELECT 1,'servicer_fee_basis','Servicer_Fee_Basis',0,NULL UNION ALL
SELECT 1,'cash_collateral_type','Cash_Collateral_Type',0,NULL UNION ALL
SELECT 1,'date_of_cash_collateral','Date_Of_Cash_Collateral',0,NULL UNION ALL
SELECT 1,'fd_bank','Fd_Bank',0,NULL UNION ALL
SELECT 1,'fd_receipt_no','Fd_Receipt_No',0,NULL UNION ALL
SELECT 1,'lien_marked','Lien_Marked',0,NULL UNION ALL
SELECT 1,'cash_collateral_basis','Cash_Collateral_Basis',0,NULL UNION ALL
SELECT 1,'reset_frequency','Reset_Frequency',0,NULL UNION ALL
SELECT 1,'deal_reference_id','Deal_Reference_Id',1,NULL UNION ALL
SELECT 1,'schedule_generation','Schedule_Generation',0,NULL UNION ALL
SELECT 1,'repayment_schedule_type','Repayment_Schedule_Type',0,NULL UNION ALL
SELECT 1,'waiver_shared','Waiver_Shared',0,NULL UNION ALL
SELECT 1,'is_lender1_interest_updated','Is_Lender1_Interest_Updated',1,NULL UNION ALL
SELECT 1,'accounting_group_name','Accounting_Group_Name',0,NULL UNION ALL
SELECT 1,'overdue_split','Overdue_Split',0,NULL UNION ALL
SELECT 1,'fee_payment_after_closure','Fee_Payment_After_Closure',0,NULL UNION ALL
SELECT 1,'fee_clawback_after_closure','Fee_Clawback_After_Closure',0,NULL UNION ALL
SELECT 1,'is_lender_roi_check_required','Is_Lender_Roi_Check_Required',0,NULL UNION ALL
SELECT 1,'max_closure_principal_variance','Max_Closure_Principal_Variance',0,NULL UNION ALL
SELECT 1,'auto_split_originator','Auto_Split_Originator',0,NULL UNION ALL
SELECT 1,'payout_report_name','Payout_Report_Name',1,NULL;





