import xml.etree.ElementTree as ET

def filter_kml_by_zip(input_kml_path, output_kml_path, target_zip_code):
    # Parse the existing KML file
    tree = ET.parse(input_kml_path)
    root = tree.getroot()

    # Define namespaces
    ns = {
        'kml': 'http://www.opengis.net/kml/2.2'
    }

    # Create a new KML file structure
    new_kml = ET.Element('kml', xmlns="http://www.opengis.net/kml/2.2")
    document = ET.SubElement(new_kml, 'Document')

    # Iterate through each Placemark in the KML file
    for placemark in root.findall('.//kml:Placemark', ns):
        name = placemark.find('kml:name', ns)
        
        if name is not None and target_zip_code in name.text:
            # If the ZIP code matches, append the Placemark to the new KML
            document.append(placemark)

    # Write the new KML file
    new_tree = ET.ElementTree(new_kml)
    new_tree.write(output_kml_path, xml_declaration=True, encoding='UTF-8')

# Specify the paths and the ZIP code
input_kml = 'zcta_boundaries.kml'  # Path to the original KML file
output_kml = 'filtered_boundaries.kml'  # Path to the new KML file
zip_code_to_filter = '08323'  # Replace with your specific ZIP code

# Call the function to filter the KML file
filter_kml_by_zip(input_kml, output_kml, zip_code_to_filter)

print(f"New KML file created: {output_kml}")
