import re
import pandas as pd
import sys
sys.path.insert(1, '../')
class HL7_Conversion():
    def __init__(self):
        pass
    def findnth(self,haystack, needle, n):
        if haystack is None:
            return -1
        parts= haystack.split(needle, n+1)


        if len(parts)<=n+1:
            return -1
        return len(haystack)-len(parts[-1])-len(needle)

    def get_string_btw_index(self, string, start_index,end_index=None):
        if end_index:
            return string[start_index:end_index]
        else:
            return string[start_index:]

    def replace_btw_index(self, string, updated_data,start_index, end_index=None):
        # print("---->", string)
        if end_index:
            if start_index == 0:
                return  updated_data + string[end_index:]
            elif start_index > 0:
                return string[:start_index+1] + updated_data + string[end_index:]
        else:

            carrot_index = string[start_index+1:].find('^')
            pipe_index = string[start_index+1:].find('|')

            if carrot_index == -1:
                carrot_index = 10000
            elif pipe_index == -1:
                pipe_index == 10000
            if carrot_index < pipe_index:
                return string[:start_index + 1] + updated_data + string[start_index + carrot_index+1:]
            elif pipe_index < carrot_index:
                return string[:start_index + 1] + updated_data + string[start_index+pipe_index+1:]
            else:
                return string[start_index+1:] + updated_data

    def insert_dash(self, string, index):
        return string[:index] + '-' + string[index:]

    def insert_data_in_hl7(self,hl7_segment, data, data_location):

        composite_index = self.get_composite_index(hl7_segment, data_location.split('.'))

        composite_data = self.get_composite(hl7_segment, composite_index[0], composite_index[1])
        # print("composite data: ",composite_data)
        if '.' in data_location:
            data_location = data_location.split('.')
            sub_composite_location = data_location[1]
            if len(data_location) == 2:
                start_index = self.findnth(composite_data, '^', int(sub_composite_location) - 2)
                end_index = self.findnth(composite_data, '^', int(sub_composite_location)-1)
                # print("sub-composite start end index: ",start_index,end_index)
                if end_index == -1 and start_index == -1:
                    # LogWriter("").write("location not found {0}".format(data_location) )
                    return None
                elif end_index == -1:
                    end_index = len(composite_data)+1
                elif start_index == -1:
                    start_index = 0
                if "VAR" in composite_data:
                    # print("---",composite_data)
                    updated_composite = composite_data.replace("VAR",data)
                    # LogWriter("").write("updated data: {0}".format(updated_composite))
                    updated_composite = hl7_segment[:composite_index[0] + 1] + updated_composite + hl7_segment[
                                                                                                   composite_index[1]:]
                else:
                    updated_composite = self.replace_btw_index(composite_data,data,start_index,end_index)

                    updated_composite = hl7_segment[:composite_index[0]+1] + updated_composite + hl7_segment[composite_index[1]:]
                return updated_composite

            elif len(data_location) == 3:
                start_index = self.findnth(composite_data, '&', int(data_location[2]) - 2)
                end_index = self.findnth(composite_data, '&', int(data_location[2]) - 1)
                # print("sub2_composite: ",start_index,end_index)

                if end_index == -1 and start_index == -1:
                    # LogWriter("").write("location not found {0}".format(data_location))
                    return None
                elif end_index == -1:
                    end_index = len(composite_data)+1
                elif start_index == -1:
                    start_index = 0

                if "VAR" in composite_data:
                    updated_composite = composite_data.replace("VAR", data)
                    updated_composite = hl7_segment[:composite_index[0] + 1] + updated_composite + hl7_segment[
                                                                                                   composite_index[1]:]
                else:
                    updated_composite = self.replace_btw_index(composite_data,data,start_index,end_index)


                    updated_composite = hl7_segment[:composite_index[0] + 1] + updated_composite + hl7_segment[
                                                                                                   composite_index[1]:]
                return updated_composite
            # LogWriter("").write('...')

        else:
            # print("----",composite_data)
            if composite_data is not None:
                if "VAR" in composite_data:
                    # print("VAR")
                    updated_hl7_segment = composite_data.replace("VAR", data)
                    updated_hl7_segment = hl7_segment[:composite_index[0] + 1] + updated_hl7_segment + hl7_segment[
                                                                                                   composite_index[1]:]
                    # print(updated_hl7_segment)
                else:
                    updated_hl7_segment = hl7_segment.replace(composite_data, data)
                    updated_hl7_segment = hl7_segment[:composite_index[0] + 1] + updated_hl7_segment + hl7_segment[
                                                                                                   composite_index[1]:]

                return updated_hl7_segment
            else:
                return None

    def extract_data_from_hl7_dict(self,hl7_segment, data_location):

        composite_location = data_location.split('.')[0]
        composite_data = self.get_composite_dict(hl7_segment, composite_location)

        if '.' in data_location:
            data_location = data_location.split('.')
            sub_composite = ''
            for key, val in composite_data.items():

                sub_composite = val[int(data_location[1]) - 1]
            '''extract only composite '''

            if len(data_location) == 2:
                return sub_composite
            elif len(data_location) > 2:
                for key, val in sub_composite.items():
                    sub2_composite = val[int(data_location[2]) - 1]
                return sub2_composite


        else:
            return composite_data

    def extract_data_from_hl7(self,hl7_segment, data_location):
        composite_index = self.get_composite_index(hl7_segment, data_location.split('.'))
        # print("composite_index:", composite_index)
        composite_data = self.get_composite(hl7_segment, composite_index[0],composite_index[1])
        # print(composite_data)

        if '.' in data_location:
            data_location = data_location.split('.')
            sub_composite_location = data_location[1]
            if len(data_location) == 2:
                start_index = self.findnth(composite_data, '^', int(sub_composite_location) - 2)
                end_index = self.findnth(composite_data, '^', int(sub_composite_location)-1)

                if end_index == -1 and start_index == -1:
                    # LogWriter("").write("location not found {0}".format(data_location),"ERROR")
                    return None
                elif end_index == -1:
                    end_index = len(composite_data)+1
                elif start_index == -1:
                    start_index = 0
                # print("sub-composite: ", start_index, end_index)
                return composite_data[start_index:end_index]

            elif len(data_location) == 3:
                start_index = self.findnth(composite_data, '&', int(data_location[2]) - 2)
                end_index = self.findnth(composite_data, '&', int(data_location[2]) - 1)
                # print("sub2_composite: ",start_index,end_index)

                if end_index == -1 and start_index == -1:
                    # print("location not found")
                    return None
                elif end_index == -1:
                    end_index = len(composite_data)+1
                    return composite_data[start_index + 1 :end_index]
                elif start_index == -1:
                    start_index = 0
                    return composite_data[start_index:end_index]




        else:
            return composite_data

    def get_composite(self,hl7_segment, start_index, end_index):
        '''extract only composite / sub_composite '''
        if end_index == -1:
            end_index = None
        composite_data = self.get_string_btw_index(hl7_segment, start_index + 1, end_index)
        if hl7_segment == composite_data:
            return None
        else:
            return composite_data


    def get_composite_dict(self,hl7_segment, data_location):
            '''extract only composite '''
            start_index =self.findnth(hl7_segment,'|',int(data_location)-1)
            end_index = self.findnth(hl7_segment,'|',int(data_location))
            if end_index == -1:
                end_index = None
            composite_data = self.get_string_btw_index(hl7_segment,start_index+1,end_index)
            if '^' in composite_data:
                data_dict = {}
                composite_data =composite_data.split('^')
                for data in composite_data:
                    if data == composite_data[0]:
                        data_dict[data] = []
                    else:
                        if data != '':
                            if '&' in data:
                                sub2_composite = data.split('&')
                                sub2_data_dict = {}
                                for sub2_data in sub2_composite:
                                    if sub2_data == sub2_composite[0]:
                                        sub2_data_dict[sub2_data] = []
                                    else:
                                        sub2_data_dict[sub2_composite[0]].append(sub2_data)
                                data_dict[composite_data[0]].append(sub2_data_dict)
                            else:
                                data_dict[composite_data[0]].append(data)
                # print(data_dict)
                return data_dict

    def get_composite_index(self,hl7_segment, data_location):
        # print(data_location)
        start_index, end_index = 0,0
        if len(data_location) == 1:
            start_index = self.findnth(hl7_segment, '|', int(data_location[0]) - 1 )
            end_index = self.findnth(hl7_segment, '|', int(data_location[0]) )
            if end_index == -1:
                end_index = len(hl7_segment) + 1
        elif len(data_location) == 2:
            start_index = self.findnth(hl7_segment, '|', int(data_location[0]) - 1 )
            end_index = self.findnth(hl7_segment, '|', int(data_location[0]) )
            if end_index == -1:
                end_index = len(hl7_segment) + 1
        elif len(data_location)==3:

            start_index = self.findnth(hl7_segment, '^', int(data_location[1]) - 2)
            end_index = self.findnth(hl7_segment, '^', int(data_location[1]) -1)
            if end_index == -1:
                end_index =self.findnth(hl7_segment, '|', int(data_location[0]))

        # print("composite index: ",[start_index,end_index])
        return [start_index,end_index]


    def count_OBX_per_OBR(self, hl7List):
        # Initialize a dictionary to store the counts
        obx_counts = {}

        current_obr = None  # Keep track of the current OBR segment
        current_obx_count = 0  # Initialize the OBX count for the current OBR

        for line in hl7List:
            segments = line

            if segments[:3] == "OBR":

                if current_obr is not None:
                    obx_counts[current_obr] = current_obx_count

                current_obr = line
                current_obx_count = 0
            elif segments[:3] == "OBX":
                current_obx_count += 1

        # Store the OBX count for the last OBR segment
        if current_obr is not None:
            obx_counts[current_obr] = current_obx_count

        return obx_counts

    def read_hl7(self, file_path, data_location):
        hl7_segments = []
        with open(file_path) as hl7_file:
            hl7_segments = list(line for line in (l.strip() for l in hl7_file) if line)

        columns = []
        data = []
        skip_char = ('FHS', 'BHS', 'BTS', 'FTS')

        for key, value in data_location.items():
            if "MSH" in value:
                location = value.split("-")[1].split(".",1)
                update_segment = int(location[0]) - 1
                if len(location) == 1:
                    location = "MSH-" + str(update_segment)
                elif len(location) == 2:
                    location = "MSH-" + str(update_segment)+"." +location[1]
                data_location[key] = location

        hl7_segments[:] = [item for item in hl7_segments if not item.startswith(skip_char)]

        extractedDataDict = {}
        data_test = []
        obr_count = sum(1 for segment in hl7_segments if segment.startswith("OBR"))
        obx_count = sum(1 for segment in hl7_segments if segment.startswith("OBX"))
        obx_counts = self.count_OBX_per_OBR(hl7_segments)
        obrFlag = False
        listIndex = -1
        for i in range(0, len(hl7_segments)):
            if hl7_segments[i][:3] == "MSH" and i > 0:
                data.append(extracted_data)
                extractedDataDict = {}

            # if hl7_segments[i][:3] == "OBR":
            #     print("obr_count",obx_counts[hl7_segments[i]])


            for key, value in data_location.items():
                if key not in columns:
                    columns.append(key)

                compare_value = value.split("-")
                if hl7_segments[i][:3] == compare_value[0]:
                    # print(compare_value)
                    location = compare_value[1]
                    extracted_data = self.extract_data_from_hl7(hl7_segments[i], str(location))

                    if extracted_data is None:
                        extracted_data = ""

                    # if extracted_data is not None:
                    if "^" in extracted_data or "&" in extracted_data or "|" in extracted_data:
                        extracted_data = extracted_data.replace("^", "").replace("&", "").replace("|", "")


                    if hl7_segments[i][:3] == "NTE" and 'CODE^V213' in hl7_segments[i]:
                        for dictionary in data_test:
                            if key not in dictionary:
                               dictionary[key] = extracted_data
                            elif key in dictionary:
                                if isinstance(dictionary[key], list):
                                    dictionary[key].append(extracted_data)
                                else:
                                    dictionary[key] = [dictionary[key], extracted_data]

                    elif hl7_segments[i][:3] == "NTE" and 'CODE^V213' not in hl7_segments[i]:
                        if len(data_test) > 0 and len(data_test) == listIndex + 1:
                            if 'obxComments' not in  data_test[listIndex]:
                                data_test[listIndex]['obxComments'] = extracted_data
                            elif 'obxComments' in  data_test[listIndex]:
                                if isinstance(data_test[listIndex]['obxComments'], list):
                                    data_test[listIndex]['obxComments'].append(extracted_data)
                                else:
                                    data_test[listIndex]['obxComments'] = [data_test[listIndex]['obxComments'], extracted_data]
                        else:
                            extractedDataDict[key] = extracted_data

                    else:
                            extractedDataDict[key] = extracted_data

            if hl7_segments[i][:3] == "OBX":
                import copy
                print("pushing data to list")
                new_data = copy.copy(extractedDataDict)
                data_test.append(new_data)
                listIndex += 1


        df = pd.DataFrame(data_test)
        return df
def getFieldLocations():
    import csv
    dict_from_csv = {}
    count = 0
    with open('field_location.csv', mode='r') as inp:
        reader = csv.reader(inp)

        for line in reader:
            if len(line) > 1:
                count += 1
                dict_from_csv[line[0].strip(' ')] = line[1].strip(' ')

    return dict_from_csv

fieldLocations = getFieldLocations()
rcvdFilePath = 'test.hl7'
fetch_data = HL7_Conversion().read_hl7(rcvdFilePath, fieldLocations)
resultData = fetch_data.to_dict('records')
test = [d['labTestId'] for d in resultData if 'labTestId' in d]
for result in resultData:
    print(result)
