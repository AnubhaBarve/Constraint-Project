import win32com.client

rfile = r'S:\trading\Nodal FTRs\ERCOT\DAM PSSE Files\DAM Cases and Supporting files\ZipTemp\DAM{}_0{}.RAW'.format(casedate,hour_pw[:2])

simauto = win32com.client.Dispatch("pwrworld.SimulatorAuto")
simauto.OpenCaseType(rfile, 'PTI')
simauto.RunSCriptCommand('EnterMode(RUN)')
simauto.RunSCriptCommand('SolvePowerFlow(DC)')

#########################################Pull load data#############################
FieldBusArray1 = ['BusNum','LoadID', 'BusName', 'ZoneName','LoadMW']
results = simauto.GetParametersMultipleElement('Load', FieldBusArray1, '')
load = pd.DataFrame()
for it in range(0, len(FieldBusArray1)):
    load[FieldBusArray1[it]] = results[1][it]
###############################Pull gen data##########################################
FieldBusArray1 = ['BusNum','GenID', 'BusName', 'ZoneName','GenMW']
results = simauto.GetParametersMultipleElement('Gen', FieldBusArray1, '')
gen = pd.DataFrame()
for it in range(0, len(FieldBusArray1)):
    gen[FieldBusArray1[it]] = results[1][it]

simauto.RunSCriptCommand('EnterMode(EDIT)')

###############################################Change Existing Loads##############################################
Loadinput=[str(int(table_loads.index[it][0])),str(int(table_loads.index[it][1])),float(abs(table_loads.iloc[it,0]))]
simauto.ChangeParametersMultipleElementFlatInput('Load',['BusNum','LoadID','LoadMW'],1,Loadinput)
load_name=LZ_loads[(LZ_loads[' PSS/E Bus Number'].astype(int)==int(table_loads.index[it][0])) &(LZ_loads[' PSS/E Load Id'].astype(int)==int(
    table_loads.index[it][1]))].reset_index().loc[0,'  Load Name']
Loadinput2=[str(int(table_loads.index[it][0])),str(int(table_loads.index[it][1])),load_name]
simauto.ChangeParametersMultipleElementFlatInput('Load', ['BusNum', 'LoadID', 'LabelAppend'], 1, Loadinput2)

###############################Add new Gen#################################################
addgenbus='CreateData(GEN, [busnum, GenID, AVR,MvarMax,MvarMin,MvarSetPoint,AGC,MWMax,MWMin,MWSetPoint,VoltSet,Status],' \
      '[{},88,YES,9900,-9900,0,YES,9900,0,{},1,Closed])'.format(str(int(table_NODE.index[it])),float(table_NODE.iloc[it,0]))
simauto.RunSCriptCommand(addgenbus)
node_name = combined_NODE[combined_NODE['BusNumber'].astype(int) == int(table_NODE.index[it])].reset_index().loc[0, 'STL_PNT']
node_input=[str(int(table_NODE.index[it])),'88',node_name]
simauto.ChangeParametersMultipleElementFlatInput('GEN', ['BusNum', 'GenID', 'LabelAppend'], 1, node_input)

######################################Add new load##############################################
addloadbus='CreateData(Load, [busnum, LoadID, SMvar,SMW,Status],[{},87,0,{},Closed])'.format(str(int(table_NODE.index[it])),
                                                                                        float(abs(table_NODE.iloc[it,0])))
simauto.RunSCriptCommand(addloadbus)
node_name=combined_NODE[combined_NODE['BusNumber'].astype(int)==int(table_NODE.index[it])].reset_index().loc[0,'STL_PNT']
node_input=[str(int(table_NODE.index[it])),'87',node_name]
simauto.ChangeParametersMultipleElementFlatInput('Load', ['BusNum', 'LoadID', 'LabelAppend'], 1, node_input)

####################save case if needed##############################################
simauto.SaveCase(output_file, 'PWB', True)
simauto.CloseCase


##########################Pull Branch Flows########################################
simauto = simulator.Simulator()
simauto.sim.OpenCaseType(rfile, 'PTI')
simauto.sim.RunSCriptCommand('EnterMode(RUN)')
simauto.sim.RunSCriptCommand('SolvePowerFlow(DC)')

results = simauto.sim.GetParametersMultipleElement('branch', config.ALL_FIELD_BUS_ARRAY, '')

# TODO: improve this, to not use a FOR loop
branches = pd.DataFrame()
for it in range(0, len(config.ALL_FIELD_BUS_ARRAY)):
    branches[config.ALL_FIELD_BUS_ARRAY[it]] = results[1][it]

#################################Close open branches##############################
# Get all the "open" branches, outages known from the RAW file
open_branches = branches[branches['LineStatus'] == 'Open']
open_branches.reset_index(inplace=True, drop=True)

# "close" all open branches, essentially ignore the outages from the RAW file
close_open_branches = []
for it in range(0, len(open_branches)):
    close_open_branches = np.append(close_open_branches,
                                    open_branches.loc[it, ['BusNum', 'BusNum:1', 'LineCircuit']].values)
    close_open_branches = np.append(close_open_branches, 'Closed')  # LineStatus
# Change the parameters using the newly closed branches
simauto.sim.ChangeParametersMultipleElementFlatInput('branch',
                                                     ['BusNum', 'BusNum:1', 'LineCircuit', 'LineStatus'],
                                                     len(open_branches), close_open_branches)

##########################LODF#############################################################
for it in range(0, len(modelbranches_closed_pjm)):
a = re.sub('\s+', '', modelbranches_closed_pjm.loc[it, 'BusNum'])
b = re.sub('\s+', '', modelbranches_closed_pjm.loc[it, 'BusNum:1'])
c = re.sub('\s+', '', modelbranches_closed_pjm.loc[it, 'LineCircuit'])
load_factor_calc_str = 'CalculateLODF([BRANCH {} {} {}],DC)'.format(a, b, c)
simauto.sim.RunScriptCommand(load_factor_calc_str)
#FieldLodf = ["BusNumFrom", "BusNumTo", "Circuit", "Label", "LODF"]
#added Label to config list

branches_output = simauto.sim.GetParametersMultipleElement("branch", config.LOAD_FACTOR_FIELDS, '')

df_temp = pd.DataFrame()
for it1 in range(0, len(config.LOAD_FACTOR_FIELDS)):
    df_temp[config.LOAD_FACTOR_FIELDS[it1]] = branches_output[1][it1]


