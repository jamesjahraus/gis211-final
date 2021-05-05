r"""GIS 211 Final

    Using arcpy.AddMessage instead of print for requirement:
    Add at least 3 print statements to echo messages to the screen that show your progress.

    Also code is commented and 'try/except' blocks are included to catch errors.

Author: James Jahraus

Project created from arcpy template (created by James Jahraus):
https://github.com/jamesjahraus/arcpy-template

ArcGIS Pro Python reference:
https://pro.arcgis.com/en/pro-app/latest/arcpy/main/arcgis-pro-arcpy-reference.htm

Error handling examples come from:
https://pro.arcgis.com/en/pro-app/latest/arcpy/get-started/error-handling-with-python.htm
"""

import os
import sys
import time
import arcpy


def pwd():
    r"""Prints the working directory.
    Used to determine the directory this module is in.

    Returns:
        The path of the directory this module is in.
    """
    wd = sys.path[0]
    arcpy.AddMessage('wd: {0}'.format(wd))
    return wd


def set_path(wd, data_path):
    r"""Joins a path to the working directory.

    Arguments:
        wd: The path of the directory this module is in.
        data_path: The suffix path to join to wd.
    Returns:
        The joined path.
    """
    path_name = os.path.join(wd, data_path)
    arcpy.AddMessage('path_name: {0}'.format(path_name))
    return path_name


def import_spatial_reference(dataset):
    r"""Extracts the spatial reference from input dataset.

    Arguments:
        dataset: Dataset with desired spatial reference.
    Returns:
        The spatial reference of any dataset input.
    """
    spatial_reference = arcpy.Describe(dataset).spatialReference
    arcpy.AddMessage('spatial_reference: {0}'.format(spatial_reference.name))
    return spatial_reference


def setup_env(workspace_path, spatial_ref_dataset):
    # Set workspace path.
    arcpy.env.workspace = workspace_path
    arcpy.AddMessage('workspace(s): {}'.format(arcpy.env.workspace))

    # Set output overwrite option.
    arcpy.env.overwriteOutput = True
    arcpy.AddMessage('overwriteOutput: {}'.format(arcpy.env.overwriteOutput))

    # Set the output spatial reference.
    arcpy.env.outputCoordinateSystem = import_spatial_reference(
        spatial_ref_dataset)
    arcpy.AddMessage('outputCoordinateSystem: {}'.format(
        arcpy.env.outputCoordinateSystem.name))


def check_status(result):
    r"""Logs the status of executing geoprocessing tools.

    Requires futher investigation to refactor this function:
        I can not find geoprocessing tool name in the result object.
        If the tool name can not be found may need to pass it in.
        Return result.getMessages() needs more thought on what it does.

    Understanding message types and severity:
    https://pro.arcgis.com/en/pro-app/arcpy/geoprocessing_and_python/message-types-and-severity.htm

    Arguments:
        result: An executing geoprocessing tool object.
    Returns:
        Requires futher investigation on what result.getMessages() means on return.
    """
    status_code = dict([(0, 'New'), (1, 'Submitted'), (2, 'Waiting'),
                        (3, 'Executing'), (4, 'Succeeded'), (5, 'Failed'),
                        (6, 'Timed Out'), (7, 'Canceling'), (8, 'Canceled'),
                        (9, 'Deleting'), (10, 'Deleted')])

    arcpy.AddMessage('current job status: {0}-{1}'.format(
        result.status, status_code[result.status]))
    # Wait until the tool completes
    while result.status < 4:
        arcpy.AddMessage('current job status (in while loop): {0}-{1}'.format(
            result.status, status_code[result.status]))
        time.sleep(0.2)
    messages = result.getMessages()
    arcpy.AddMessage('job messages: {0}'.format(messages))
    return messages


def function_template():
    r"""Function summary.
    Description sentence(s).
    Arguments:
        arg 1: Description sentence.
        arg 2: Description sentence.
    Returns:
        Description sentence.
    Raises:
        Description sentence.
    """
    pass


def flush_db(db):
    # Delete the contents of a geodatabase.
    arcpy.AddMessage(f'\nFlushing db: {db}')
    arcpy.env.workspace = db
    arcpy.AddMessage(f'Contents of db Before Flush {arcpy.ListFeatureClasses()}')
    for fc in arcpy.ListFeatureClasses():
        arcpy.AddMessage(f'Deleting: {fc}')
        if arcpy.Exists(fc):
            arcpy.Delete_management(fc)
    arcpy.AddMessage(f'Contents of db After Flush {arcpy.ListFeatureClasses()}\n')


def copy_env_db(to_db):
    # Copy a geodatabase to another geodatabase.
    # Assumes the environment db is set before being called.
    fclist = arcpy.ListFeatureClasses()
    for fc in fclist:
        if arcpy.Exists(fc):
            arcpy.AddMessage(f'Found {fc} copying to output db.')
            arcpy.CopyFeatures_management(fc, set_path(to_db, f'{fc}_copy'))
        else:
            arcpy.AddMessage(f'{fc} does not exist in db.')


def copy_fc(fc, to_db, suffix='copy'):
    # Copy a fc to a db and adds a suffix to the output feature class.
    # Includes error handing:
    # AddMessage for what I think the expected behaviour should be.
    # AddError to display actual error.
    # Reference: https://pro.arcgis.com/en/pro-app/latest/tool-reference/data-management/copy-features.htm
    arcpy.AddMessage('\nStarting copy_fc')
    try:
        output_fc = set_path(to_db, f'{fc}_{suffix}')
        result = arcpy.CopyFeatures_management(fc, output_fc)
        check_status(result)
        arcpy.AddMessage(f'Found {fc} copying to {to_db} db.')
    except arcpy.ExecuteError:
        arcpy.AddMessage(f'{fc} does not exist in db.')
        arcpy.AddError(arcpy.GetMessages(2))
    arcpy.AddMessage('copy_fc complete\n')
    return output_fc


def fc_to_layer(fc, lyr_name):
    # Converts a feature class to a layer for geoprocessing tools that require a layer.
    # Side effect is a layer with lyr_name will exist in the defaut workspace.
    # Assumes setup_env was called to setup workspace.
    # Reference: https://pro.arcgis.com/en/pro-app/latest/tool-reference/data-management/make-feature-layer.htm
    arcpy.AddMessage('\nStarting fc_to_layer')
    try:
        result = arcpy.MakeFeatureLayer_management(fc, lyr_name)
        check_status(result)
        arcpy.AddMessage(f'{lyr_name} now exists in workspace.')
    except arcpy.ExecuteError:
        arcpy.AddError(arcpy.GetMessages(2))
    arcpy.AddMessage('fc_to_layer complete\n')


def multiple_ring_buffer(in_fc, out_fc, distances, buffer_unit):
    # Creates a multi ring buffer output feature class from input feature class.
    # Reference: https://pro.arcgis.com/en/pro-app/latest/tool-reference/analysis/multiple-ring-buffer.htm
    arcpy.AddMessage('\nStarting multiple_ring_buffer')
    try:
        result = arcpy.MultipleRingBuffer_analysis(in_fc, out_fc, distances, buffer_unit, "", "ALL")
        check_status(result)
    except arcpy.ExecuteError:
        arcpy.AddError(arcpy.GetMessages(2))
    arcpy.AddMessage('multiple_ring_buffer complete\n')


def add_field(input_fc, field_name, field_type):
    # Adds a new field to the input_fc using only the options field_name and field_type.
    # Reference: https://pro.arcgis.com/en/pro-app/latest/tool-reference/data-management/add-field.htm
    arcpy.AddMessage('\nStarting add_field')
    try:
        result = arcpy.AddField_management(input_fc, field_name, field_type, '', '', field_is_nullable="NULLABLE")
        check_status(result)
    except arcpy.ExecuteError:
        arcpy.AddError(arcpy.GetMessages(2))
    arcpy.AddMessage('add_field complete\n')


def calculate_field(input_fc, field_name, expression, code_block):
    # Performs a python 3 field calculation on a feature class / field using input expression and code block.
    # Reference: https://pro.arcgis.com/en/pro-app/latest/tool-reference/data-management/calculate-field.htm
    arcpy.AddMessage('\nStarting calculate_field')
    try:
        result = arcpy.CalculateField_management(input_fc, field_name, expression, "PYTHON3", code_block)
        check_status(result)
    except arcpy.ExecuteError:
        arcpy.AddError(arcpy.GetMessages(2))
    arcpy.AddMessage('calculate_field complete\n')


def union(fc_list, output_fc):
    # Performs a union of all the feature classes in the input feature class list.
    # join attributes are "ALL", no cluster tolerance, and gaps set to "GAPS".
    # Reference: https://pro.arcgis.com/en/pro-app/latest/tool-reference/analysis/union.htm
    arcpy.AddMessage('\nStarting union')
    try:
        result = arcpy.Union_analysis(fc_list, output_fc, "ALL", "", "GAPS")
        check_status(result)
    except arcpy.ExecuteError:
        arcpy.AddError(arcpy.GetMessages(2))
    arcpy.AddMessage('union complete\n')


def main(flush_output_db=False):
    r"""GIS 211 Final
    """
    # Setup Geoprocessing Environment
    spatial_ref_dataset = 'LongmontRecCenter'
    wd = pwd()
    input_db = set_path(wd, 'gis211-final.gdb')
    output_db = set_path(wd, 'gis211-final_output.gdb')

    # If flush_output_db param is set to True, flush the output db.
    if flush_output_db:
        flush_db(output_db)

    setup_env(input_db, spatial_ref_dataset)

    # Check db path is setup correctly by performing a copy from env db to output_db.
    # copy_env_db(output_db)

    # Longmont Rec Center
    # Create a multi ring buffer around the Rec Center feature class.
    # Add rank field and calculate the BufferRank.
    # See function comments for details about each function call.
    buf_input_fc = copy_fc('LongmontRecCenter', output_db, 'pycharm')
    buf_input_lyr = 'LongmontRecCenter_lyr'
    fc_to_layer(buf_input_fc, buf_input_lyr)
    buf_output_fc = set_path(output_db, 'RecCenterBuffers_pycharm')
    multiple_ring_buffer(buf_input_lyr, buf_output_fc, [1, 2, 2.5, 6], 'Miles')
    buf_field_name = 'BufferRank'
    add_field(buf_output_fc, buf_field_name, 'SHORT')
    buffer_rank_expression = "CalculateIF(!distance!, !BufferRank!)"
    buffer_rank_code_block = """
def CalculateIF(distance, BufferRank):
    if (distance > 0 and distance <= 1):
        return 1
    elif (distance > 1 and distance <= 2):
        return 2
    elif (distance > 2 and distance <= 2.5):
        return 4
    elif (distance > 2.5):
        return 5
"""
    calculate_field(buf_output_fc, buf_field_name, buffer_rank_expression, buffer_rank_code_block)

    # Longmont Zoning
    # Add rank field and calculate the ZoningRank.
    # See function comments for details about each function call.
    zoning_output_fc = copy_fc('LongmontZoning', output_db, 'pycharm')
    zoning_field_name = 'ZoningRank'
    add_field(zoning_output_fc, zoning_field_name, 'SHORT')
    zoning_rank_expression = "CalculateIF( !ZONE_NAME!, !ZoningRank!)"
    zoning_rank_code_block = """
def CalculateIF(Zone_Name, ZoningRank):
    if (Zone_Name == 'Business / Light Industrial' or
            Zone_Name == 'Commercial' or
            Zone_Name == 'Commercial - Regional' or
            Zone_Name == 'General Industrial' or
            Zone_Name == 'Commercial Planned Unit Development' or
            Zone_Name == 'Industrial Planned Unit Development' or
            Zone_Name == 'Mixed Industrial' or
            Zone_Name == 'Public'):
        return 5
    elif (Zone_Name == 'Agriculture' or
          Zone_Name == 'Central Business District' or
          Zone_Name == 'Mixed Use Planned Unit Development'):
        return 4
    else:
        return 0
"""
    calculate_field(zoning_output_fc, zoning_field_name, zoning_rank_expression, zoning_rank_code_block)

    # Population Ellipse Buffered
    # Add rank field and calculate the PopRank.
    # See function comments for details about each function call.
    pop_output_fc = copy_fc('PopulationEllipse_Buffered', output_db, 'pycharm')
    pop_field_name = 'PopRank'
    add_field(pop_output_fc, pop_field_name, 'SHORT')
    pop_rank_expression = "CalculateIF( !ToBufDist!, !PopRank!)"
    pop_rank_code_block = """
def CalculateIF(distance, PopRank):
    if distance == 0:
        return 5
    elif distance == .5:
        return 4
    else:
        return 3
"""
    calculate_field(pop_output_fc, pop_field_name, pop_rank_expression, pop_rank_code_block)

    # Union
    # See function comments for details about each function call.
    union_fc_list = [buf_output_fc, zoning_output_fc, pop_output_fc]
    union_output_fc = set_path(output_db, 'UnionZoneBuffPop_pycharm')
    union(union_fc_list, union_output_fc)


if __name__ == '__main__':
    main(flush_output_db=True)
