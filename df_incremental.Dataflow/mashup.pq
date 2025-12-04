[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[IncrementalRefreshSettings = [FilterColumnName = "time", WindowSize = 4, WindowUnit = "Year", BucketUnit = "Year", BookmarkColumnName = "time", ConsiderOnlyConcludedPeriods = true, RequireIncrementalRefreshQueryToFold = false], DataDestinations = {[Definition = [Kind = "Reference", QueryName = "minciencia_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "time", DestinationColumnName = "time"], [SourceColumnName = "dd_Valor", DestinationColumnName = "dd_Valor"], [SourceColumnName = "ff_Valor", DestinationColumnName = "ff_Valor"], [SourceColumnName = "VRB_Valor", DestinationColumnName = "VRB_Valor"], [SourceColumnName = "CodigoNacional", DestinationColumnName = "CodigoNacional"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared minciencia = let
  Origen = Lakehouse.Contents([HierarchicalNavigation = null]),
  #"Navegación 1" = Origen{[workspaceId = "0937df67-f304-4d29-bfd4-c5a2bc260840"]}[Data],
  #"Navegación 2" = #"Navegación 1"{[lakehouseId = "ee3a5cef-859c-4f37-965e-cba92833f8b7"]}[Data],
  #"Navegación 3" = #"Navegación 2"{[Id = "minciencia", ItemKind = "Table"]}[Data],
  #"Columnas quitadas" = Table.RemoveColumns(#"Navegación 3", {"latitud", "longitud", "nombreEstacion"}),
  #"Filas filtradas" = Table.SelectRows(#"Columnas quitadas", each [time] >= RangeStart and [time] < RangeEnd)
in
  #"Filas filtradas";
shared RangeStart = null meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type datetime];
shared RangeEnd = null meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type datetime];
shared minciencia_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "0937df67-f304-4d29-bfd4-c5a2bc260840"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "7199137e-ea3a-4d45-87ba-9fafcc3f4154"]}[Data],
  TableNavigation = Navigation_2{[Id = "mciencia_incremental", ItemKind = "Table"]}[Data]
in
  TableNavigation;
