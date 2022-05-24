WITH 

IsICTOrderLink1000 AS (
  SELECT PrimaryKey, OrderNumber, OrderRowNumber, TRUE AS Flag 
  FROM `akbm-datawarehouse-operational.views_jeeves.FactCustomerOrders` fco
  WHERE LegalEntityId = '1000'
    AND OrderRowStatusId IN (10, 13, 40)
    AND NOT FromOrderNoICT IS NULL
),
IsICTOrderLink1100 AS (
  SELECT PrimaryKey, OrderNumber, OrderRowNumber, TRUE AS Flag 
  FROM `akbm-datawarehouse-operational.views_jeeves.FactCustomerOrders` fco
  WHERE LegalEntityId = '1100'
    AND NOT FromOrderNoICT IS NULL 
),
IsICTOrderLink1550 AS (
  SELECT PrimaryKey, OrderNumber, OrderRowNumber, TRUE AS Flag 
  FROM `akbm-datawarehouse-operational.views_jeeves.FactCustomerOrders` fco
  WHERE LegalEntityId = '1550'
    AND OrderRowStatusId IN (10, 13, 40)
    AND NOT OrderNumber IS NULL
),
IsFull AS (
  SELECT PrimaryKey, OrderNumber, OrderRowNumber, TRUE AS Flag 
  FROM `akbm-datawarehouse-operational.views_jeeves.FactCustomerOrders` fco
  WHERE fco.LegalEntityId IN ('1000', '1100', '1150', '1500', '1570', '1730', '1750')
),
IsQrill AS (
  SELECT PrimaryKey, OrderNumber, OrderRowNumber, TRUE AS Flag
  FROM `akbm-datawarehouse-operational.views_jeeves.FactCustomerOrders` fco
  WHERE 
    fco.LegalEntityId IN ('1000', '1100', '1150', '1500', '1570', '1750', '1730')
    AND CAST(fco.EstimatedDeliveryDate AS DATETIME) > DATE(2018, 1, 1)
    AND fco.OrderNumber NOT IN (2081001, 2081002, 2081003)
),
IsSuperba AS (
  SELECT PrimaryKey, OrderNumber, OrderRowNumber, TRUE AS Flag
  FROM `akbm-datawarehouse-operational.views_jeeves.FactCustomerOrders` fco
  WHERE 
    fco.LegalEntityId IN ('1000', '1100', '1550')
    AND (CAST(fco.EstimatedDeliveryDate AS DATETIME) >= DATE(2018, 1, 1))
    AND fco.BackOrderNumber = 0
)

SELECT 
  fco.*,
  -- fco.PrimaryKey,
  -- fco.LegalEntityId, 
  -- fco.OrderNumber, 
  -- fco.OrderRowNumber, 
  -- fco.BackOrderNumber, 
  -- fco.OrderRowStatusId, 
  -- fco.OrderDate, 
  -- fco.CustomerOrInternal, 
  -- fco.IctStatus, 
  -- fco.ToOrderNoICT, 
  -- fco.FromOrderNoICT, 
  -- fco.AllocationStatus, 
  -- fco.ItemNumber, 
  -- fco.Kg, 
  -- fco.EstimatedDeliveryDate, 
  -- fco.FK_Item, 
  -- fco.FK_CustomerOrderHeaders, 
  -- fco.FK_Company, 
  -- fco.FK_Warehouse, 
  -- fco.FK_Inventory, 
  -- fco.FK_DispatchMethod,

  IF(COALESCE(fco.IctStatus,fco.AllocationStatus) = 0, NULL, COALESCE(fco.IctStatus,fco.AllocationStatus)) AS AllocatedCodeA,
  fcoh.CustomerCareRepresentative,
  CONCAT(fco.LegalEntityId, fco.OrderNumber, fco.OrderRowNumber, fco.ItemNumber) AS ICT1550OrderId,
  CONCAT(fco.LegalEntityId, fco.DeliveryNoteId) AS FK_DeliveryNote, 
  fdn.DestinationCountry,
  fdn.DeliveryNoteNumber AS DeliveryNoteNo,
  IF(CAST(fco.EstimatedDeliveryDate AS DATE) < CURRENT_DATE(), 'Passed EDD', 'In progress') AS DeliveryStatus,
  IF(DATE(fco.OrderDate) > DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH), 'Applicable', 'Not Applicable') AS Last6Months,
  COALESCE(IsICTOrderLink1000.Flag, FALSE) AS IsICTOrderLink1000,
  COALESCE(IsICTOrderLink1100.Flag, FALSE) AS IsICTOrderLink1100,
  COALESCE(IsICTOrderLink1550.Flag, FALSE) AS IsICTOrderLink1550,
  COALESCE(IsFull.Flag, FALSE) IsFull,
  IF(COALESCE(IsSuperba.Flag, FALSE) AND (
    COALESCE(IsICTOrderLink1000.Flag, FALSE) OR 
    COALESCE(IsICTOrderLink1100.Flag, FALSE) OR
    COALESCE(IsICTOrderLink1550.Flag, FALSE)), TRUE, FALSE) AS IsSuperba,
  COALESCE(IsQrill.Flag, FALSE) IsQrill,
  FORMAT_DATE('%b', fco.OrderDate) AS MonthName,
  IF(CAST(fco.EstimatedDeliveryDate AS DATE) < CURRENT_DATE(), IF(fdn.EstimatedDispatchDate != fco.EstimatedDeliveryDate, fdn.EstimatedDispatchDate, NULL), NULL) AS ReestimatedDispatchDate,
  CONCAT(fco.LegalEntityId, fco.OrderNumber, fco.OrderRowNumber)  AS UniqueOrderRow,
  --fco.FromOrderNoICT, --Renamed to ICT Order No. (Link 1000-1550)
  di.Product AS ProductNameDimItem, -- renamed to ProductName - should be taken from DimItem by relation
  fcoh.Weight AS WeightExt,
  

FROM `akbm-datawarehouse-operational.views_jeeves.FactCustomerOrders` fco
  LEFT JOIN IsICTOrderLink1000 ON IsICTOrderLink1000.PrimaryKey = fco.PrimaryKey AND IsICTOrderLink1000.OrderNumber = fco.OrderNumber AND IsICTOrderLink1000.OrderRowNumber = fco.OrderRowNumber
  LEFT JOIN IsICTOrderLink1100 ON IsICTOrderLink1100.PrimaryKey = fco.PrimaryKey AND IsICTOrderLink1100.OrderNumber = fco.OrderNumber AND IsICTOrderLink1100.OrderRowNumber = fco.OrderRowNumber
  LEFT JOIN IsICTOrderLink1550 ON IsICTOrderLink1550.PrimaryKey = fco.PrimaryKey AND IsICTOrderLink1550.OrderNumber = fco.OrderNumber AND IsICTOrderLink1550.OrderRowNumber = fco.OrderRowNumber
  LEFT JOIN `akbm-datawarehouse-operational.views_jeeves.FactDeliveryNotes` fdn ON fdn.PrimaryKey = CONCAT(fco.LegalEntityId, fco.DeliveryNoteId)
  LEFT JOIN `akbm-datawarehouse-operational.views_jeeves.FactCustomerOrderHeaders` fcoh ON fcoh.PrimaryKey = fco.FK_CustomerOrderHeaders
  LEFT JOIN `akbm-datawarehouse-operational.views_mdm.DimItem` di ON di.PrimaryKey = fco.FK_Item
  LEFT JOIN IsSuperba ON IsSuperba.PrimaryKey = fco.PrimaryKey AND IsSuperba.OrderNumber = fco.OrderNumber AND IsSuperba.OrderRowNumber = fco.OrderRowNumber
  LEFT JOIN IsQrill ON IsQrill.PrimaryKey = fco.PrimaryKey AND IsQrill.OrderNumber = fco.OrderNumber AND IsQrill.OrderRowNumber = fco.OrderRowNumber
  LEFT JOIN IsFull ON IsFull.PrimaryKey = fco.PrimaryKey AND IsFull.OrderNumber = fco.OrderNumber AND IsFull.OrderRowNumber = fco.OrderRowNumber
  
WHERE CAST(fco.EstimatedDeliveryDate AS DATETIME) >= DATE(2018, 1, 1)

