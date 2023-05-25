use color_eyre::eyre::Result;
use flate2::bufread::GzDecoder;
use git2::{Repository, Signature, Time};
use quick_xml::{
    events::{BytesStart, Event},
    name::QName,
    Reader,
};
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    collections::BTreeMap,
    convert::Infallible,
    fs::{File, OpenOptions},
    io::{Read, Write},
};
use time::{format_description::well_known::Iso8601, OffsetDateTime};
use tracing::{debug, error, info, warn};

use crate::git::commit;

use super::changesets::{parse_changeset, uncompress_changeset_file, Changeset};

const FILE_VERSION: &str = "0.1.0";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Node {
    /// The id of the node. Saved as the file name.
    #[serde(skip)]
    pub id: u64,
    #[serde(skip)]
    pub changeset: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_generator: Option<String>,
    pub file_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub legacy_object_version: Option<String>,
    pub lat: f64,
    pub lon: f64,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
}
impl Node {
    fn new_from_element(reader: &mut Reader<&[u8]>, element: &BytesStart) -> Result<Self> {
        let attributes: BTreeMap<String, String> = element
            .attributes()
            .filter_map(|attr_result| attr_result.ok())
            .map(|attr| {
                let key = reader
                    .decoder()
                    .decode(attr.key.local_name().as_ref())
                    .or_else(|err| {
                        dbg!(
                            "unable to read key in DefaultSettings attribute {:?}, utf8 error {:?}",
                            &attr,
                            err
                        );
                        Ok::<Cow<'_, str>, Infallible>(std::borrow::Cow::from(""))
                    })
                    .unwrap()
                    .to_string();
                let value = attr
                    .decode_and_unescape_value(reader)
                    .or_else(|err| {
                        dbg!(
                            "unable to read key in DefaultSettings attribute {:?}, utf8 error {:?}",
                            &attr,
                            err
                        );
                        Ok::<Cow<'_, str>, Infallible>(std::borrow::Cow::from(""))
                    })
                    .unwrap()
                    .to_string();
                (key, value)
            })
            .collect();

        let mut node = Node {
            id: attributes
                .get("id")
                .unwrap()
                .parse::<u64>()
                .expect("Unable to parse node id"),
            changeset: attributes
                .get("changeset")
                .unwrap()
                .parse::<u64>()
                .expect("Unable to parse node changeset"),
            file_generator: attributes.get("generator").map(|s| s.to_string()),
            legacy_object_version: attributes.get("version").map(|s| s.to_string()),
            lat: attributes
                .get("lat")
                .unwrap()
                .parse::<f64>()
                .expect("Unable to parse node lat"),
            lon: attributes
                .get("lon")
                .unwrap()
                .parse::<f64>()
                .expect("Unable to parse node lon"),
            tags: BTreeMap::new(),
            file_version: FILE_VERSION.to_string(),
        };

        let mut buf = Vec::new();
        loop {
            let event = reader.read_event_into(&mut buf)?;

            if let Event::End(ref e) = event {
                if e.name() == element.name() {
                    break;
                }
            }

            if let Event::Start(ref e) = event {
                let name = e.name();
                if name == QName(b"tag") {
                    let mut key = Cow::Borrowed("");
                    let mut value = Cow::Borrowed("");

                    for attr_result in e.attributes() {
                        let a = attr_result?;
                        match a.key.as_ref() {
                            b"k" => key = a.decode_and_unescape_value(reader)?,
                            b"v" => value = a.decode_and_unescape_value(reader)?,
                            _ => (),
                        }
                    }

                    node.tags.insert(key.to_string(), value.to_string());
                } else {
                    warn!("Unexpected tag: {:?}", name);
                }
                reader.read_to_end(name)?;
            } else {
                if let Event::Text(ref text) = event {
                    if text.borrow().starts_with(b"\n") {
                        continue;
                    }
                } else if let Event::End(ref e) = event {
                    if e.name() == QName(b"tag") {
                        continue;
                    }
                }
                warn!("Unexpected event in node: {:?}", event);
                // Write the data to file for debugging

                let mut file = std::fs::File::create("debug.xml")?;
                file.write_all(&buf)?;
                file.sync_all()?;
            }
            buf = Vec::new();
        }

        Ok(node)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Way {
    /// The id of the node. Saved as the file name.
    #[serde(skip)]
    pub id: u64,
    #[serde(skip)]
    pub changeset: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_generator: Option<String>,
    pub file_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub legacy_object_version: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub nodes: Vec<u64>,
}

impl Way {
    fn new_from_element(reader: &mut Reader<&[u8]>, element: &BytesStart) -> Result<Self> {
        let attributes: BTreeMap<String, String> = element
            .attributes()
            .filter_map(|attr_result| attr_result.ok())
            .map(|attr| {
                let key = reader
                    .decoder()
                    .decode(attr.key.local_name().as_ref())
                    .or_else(|err| {
                        dbg!(
                            "unable to read key in DefaultSettings attribute {:?}, utf8 error {:?}",
                            &attr,
                            err
                        );
                        Ok::<Cow<'_, str>, Infallible>(std::borrow::Cow::from(""))
                    })
                    .unwrap()
                    .to_string();
                let value = attr
                    .decode_and_unescape_value(reader)
                    .or_else(|err| {
                        dbg!(
                            "unable to read key in DefaultSettings attribute {:?}, utf8 error {:?}",
                            &attr,
                            err
                        );
                        Ok::<Cow<'_, str>, Infallible>(std::borrow::Cow::from(""))
                    })
                    .unwrap()
                    .to_string();
                (key, value)
            })
            .collect();

        let mut way = Way {
            id: attributes
                .get("id")
                .unwrap()
                .parse::<u64>()
                .expect("Unable to parse way id"),
            changeset: attributes
                .get("changeset")
                .unwrap()
                .parse::<u64>()
                .expect("Unable to parse way changeset"),
            file_generator: attributes.get("generator").map(|s| s.to_string()),
            legacy_object_version: attributes.get("version").map(|s| s.to_string()),
            tags: BTreeMap::new(),
            nodes: Vec::new(),
            file_version: FILE_VERSION.to_string(),
        };

        let mut buf = Vec::new();
        loop {
            let event = reader.read_event_into(&mut buf)?;

            if let Event::End(ref e) = event {
                if e.name() == element.name() {
                    break;
                }
            }

            if let Event::Start(ref e) = event {
                let name = e.name();
                if name == QName(b"tag") {
                    let mut key = Cow::Borrowed("");
                    let mut value = Cow::Borrowed("");

                    for attr_result in e.attributes() {
                        let a = attr_result?;
                        match a.key.as_ref() {
                            b"k" => key = a.decode_and_unescape_value(reader)?,
                            b"v" => value = a.decode_and_unescape_value(reader)?,
                            _ => (),
                        }
                    }

                    way.tags.insert(key.to_string(), value.to_string());
                } else if name == QName(b"nd") {
                    let mut ref_id = Cow::Borrowed("");

                    for attr_result in e.attributes() {
                        let a = attr_result?;
                        if let b"ref" = a.key.as_ref() {
                            ref_id = a.decode_and_unescape_value(reader)?;
                        }
                    }

                    way.nodes.push(
                        ref_id
                            .to_string()
                            .parse::<u64>()
                            .expect("Unable to parse way node ref"),
                    );
                } else {
                    warn!("Unexpected tag: {:?}", name);
                }
                reader.read_to_end(name)?;
            } else {
                if let Event::Text(ref text) = event {
                    if text.borrow().starts_with(b"\n") {
                        continue;
                    }
                } else if let Event::End(ref e) = event {
                    if e.name() == QName(b"tag") || e.name() == QName(b"nd") {
                        continue;
                    }
                }
                warn!("Unexpected event way: {:?}", event);
                // Write the data to file for debugging

                let mut file = std::fs::File::create("debug.xml")?;
                file.write_all(&buf)?;
                file.sync_all()?;
            }
            buf = Vec::new();
        }

        Ok(way)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RelationMember {
    #[serde(rename = "type")]
    pub r#type: String,
    #[serde(rename = "ref")]
    pub ref_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Relation {
    /// The id of the node. Saved as the file name.
    #[serde(skip)]
    pub id: u64,
    #[serde(skip)]
    pub changeset: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_generator: Option<String>,
    pub file_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub legacy_object_version: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub member: Vec<RelationMember>,
}

impl Relation {
    fn new_from_element(reader: &mut Reader<&[u8]>, element: &BytesStart) -> Result<Self> {
        let attributes: BTreeMap<String, String> = element
            .attributes()
            .filter_map(|attr_result| attr_result.ok())
            .map(|attr| {
                let key = reader
                    .decoder()
                    .decode(attr.key.local_name().as_ref())
                    .or_else(|err| {
                        dbg!(
                            "unable to read key in DefaultSettings attribute {:?}, utf8 error {:?}",
                            &attr,
                            err
                        );
                        Ok::<Cow<'_, str>, Infallible>(std::borrow::Cow::from(""))
                    })
                    .unwrap()
                    .to_string();
                let value = attr
                    .decode_and_unescape_value(reader)
                    .or_else(|err| {
                        dbg!(
                            "unable to read key in DefaultSettings attribute {:?}, utf8 error {:?}",
                            &attr,
                            err
                        );
                        Ok::<Cow<'_, str>, Infallible>(std::borrow::Cow::from(""))
                    })
                    .unwrap()
                    .to_string();
                (key, value)
            })
            .collect();

        let mut relation = Relation {
            id: attributes
                .get("id")
                .unwrap()
                .parse::<u64>()
                .expect("Unable to parse way id"),
            changeset: attributes
                .get("changeset")
                .unwrap()
                .parse::<u64>()
                .expect("Unable to parse way changeset"),
            file_generator: attributes.get("generator").map(|s| s.to_string()),
            legacy_object_version: attributes.get("version").map(|s| s.to_string()),
            tags: BTreeMap::new(),
            member: Vec::new(),
            file_version: FILE_VERSION.to_string(),
        };

        let mut buf = Vec::new();
        loop {
            let event = reader.read_event_into(&mut buf)?;

            if let Event::End(ref e) = event {
                if e.name() == element.name() {
                    break;
                }
            }

            if let Event::Start(ref e) = event {
                let name = e.name();
                if name == QName(b"tag") {
                    let mut key = Cow::Borrowed("");
                    let mut value = Cow::Borrowed("");

                    for attr_result in e.attributes() {
                        let a = attr_result?;
                        match a.key.as_ref() {
                            b"k" => key = a.decode_and_unescape_value(reader)?,
                            b"v" => value = a.decode_and_unescape_value(reader)?,
                            _ => (),
                        }
                    }

                    relation.tags.insert(key.to_string(), value.to_string());
                } else if name == QName(b"member") {
                    let mut ref_id = Cow::Borrowed("");
                    let mut r#type = Cow::Borrowed("");
                    let mut role = Cow::Borrowed("");

                    for attr_result in e.attributes() {
                        let a = attr_result?;
                        match a.key.as_ref() {
                            b"ref" => ref_id = a.decode_and_unescape_value(reader)?,
                            b"type" => r#type = a.decode_and_unescape_value(reader)?,
                            b"role" => role = a.decode_and_unescape_value(reader)?,
                            _ => (),
                        }
                    }

                    let normalized_role = if role.is_empty() {
                        None
                    } else {
                        Some(role.to_string())
                    };

                    relation.member.push(RelationMember {
                        r#type: r#type.to_string(),
                        ref_id: ref_id
                            .to_string()
                            .parse::<u64>()
                            .expect("Unable to parse relation member ref"),
                        role: normalized_role,
                    });
                } else {
                    warn!("Unexpected tag: {:?}", name);
                }
                reader.read_to_end(name)?;
            } else {
                if let Event::Text(ref text) = event {
                    if text.borrow().starts_with(b"\n") {
                        continue;
                    }
                } else if let Event::End(ref e) = event {
                    if e.name() == QName(b"tag") || e.name() == QName(b"member") {
                        continue;
                    }
                }
                warn!("Unexpected event in Relation: {:?}", event);
                // Write the data to file for debugging

                let mut file = std::fs::File::create("debug.xml")?;
                file.write_all(&buf)?;
                file.sync_all()?;
            }
            buf = Vec::new();
        }

        Ok(relation)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum OSMObject {
    Node(Node),
    Way(Way),
    Relation(Relation),
}

pub fn convert_objects_to_git(
    repository: &Repository,
    committer: &Signature,
    data: &[u8],
    changesets_location: &str,
) -> Result<()> {
    // If the file is empty we skip it
    if data.is_empty() {
        return Ok(());
    }

    // Decompress the changeset file
    let mut data_reader = GzDecoder::new(data);
    let mut file_data = String::new();
    if let Err(e) = data_reader.read_to_string(&mut file_data) {
        error!("Unable to decompress data file: {:?}. Moving on", e);
        return Ok(());
    }
    debug!("Data file decompressed. Size: {}", file_data.len());

    // If the file is empty we skip it
    if file_data.is_empty() {
        return Ok(());
    }

    info!("Parsing data file");

    let mut data = Reader::from_str(&file_data);

    // == Handling empty elements ==
    // To simply our processing code
    // we want the same events for empty elements, like:
    //   <DefaultSettings Language="es" Greeting="HELLO"/>
    //   <Text/>
    data.expand_empty_elements(true);

    let mut buf = Vec::new();
    let mut skip_buf = Vec::new();
    let mut created_or_modified_objects_for_changeset = BTreeMap::new();
    let mut deleted_objects_for_changeset = BTreeMap::new();

    loop {
        let event: Event = data.read_event_into(&mut buf)?;
        match event {
            Event::Start(element) => match element.name().as_ref() {
                b"create" => {
                    // TODO: What do we do in case of an error?

                    let mut created_objects = Vec::new();

                    loop {
                        let event = data.read_event_into(&mut skip_buf)?;

                        if let Event::End(ref e) = event {
                            if e.name() == element.name() {
                                break;
                            }
                        }

                        if let Event::Start(ref e) = event {
                            let name = e.name();
                            if name == QName(b"node") {
                                let node = Node::new_from_element(&mut data, e);
                                match node {
                                    Ok(node) => created_objects.push(OSMObject::Node(node)),
                                    Err(err) => {
                                        error!(
                                            "unable to read node element {:?}, utf8 error {:?}",
                                            &e, err
                                        );
                                    }
                                }
                            } else if name == QName(b"way") {
                                let way = Way::new_from_element(&mut data, &e);
                                match way {
                                    Ok(way) => created_objects.push(OSMObject::Way(way)),
                                    Err(err) => {
                                        error!(
                                            "unable to read way element {:?}, utf8 error {:?}",
                                            &e, err
                                        );
                                    }
                                }
                            } else if name == QName(b"relation") {
                                let relation = Relation::new_from_element(&mut data, &e);
                                match relation {
                                    Ok(relation) => {
                                        created_objects.push(OSMObject::Relation(relation))
                                    }
                                    Err(err) => {
                                        error!(
                                            "unable to read relation element {:?}, utf8 error {:?}",
                                            &e, err
                                        );
                                    }
                                }
                            } else {
                                warn!("Unexpected tag: {:?}", name);
                                data.read_to_end(name)?;
                            }
                        } else {
                            if let Event::Text(ref text) = event {
                                if text.borrow().starts_with(b"\n") {
                                    continue;
                                }
                            }
                            warn!("Unexpected event in create: {:?}", event);
                            // Write the data to file for debugging

                            let mut file = std::fs::File::create("debug.xml")?;
                            file.write_all(file_data.as_bytes())?;
                            file.sync_all()?;
                        }
                        skip_buf = Vec::new();
                    }

                    // write the objects to the git repo as yaml files
                    let repository_folder = repository.path().parent().unwrap();
                    // TODO: We should chunk the world and split it into folders... Otherwise good luck
                    for object in created_objects {
                        let object_file_name = match object {
                            OSMObject::Node(ref node) => format!("{}.yaml", node.id),
                            OSMObject::Way(ref way) => format!("{}.yaml", way.id),
                            OSMObject::Relation(ref relation) => format!("{}.yaml", relation.id),
                        };
                        let object_file_path = repository_folder.join(object_file_name);
                        let object_file = std::fs::File::create(object_file_path)?;
                        serde_yaml::to_writer(object_file, &object)?;

                        // Add the object to the list of created objects for the changeset based on the changeset id
                        let changeset = match object {
                            OSMObject::Node(ref node) => node.changeset,
                            OSMObject::Way(ref way) => way.changeset,
                            OSMObject::Relation(ref relation) => relation.changeset,
                        };
                        created_or_modified_objects_for_changeset
                            .entry(changeset)
                            .or_insert_with(Vec::new)
                            .push(object);
                    }
                }
                b"modify" => {
                    // TODO: What do we do in case of an error?

                    let mut deleted_objects = Vec::new();

                    loop {
                        let event = data.read_event_into(&mut skip_buf)?;

                        if let Event::End(ref e) = event {
                            if e.name() == element.name() {
                                break;
                            }
                        }

                        if let Event::Start(ref e) = event {
                            let name = e.name();
                            if name == QName(b"node") {
                                let node = Node::new_from_element(&mut data, &e);
                                match node {
                                    Ok(node) => deleted_objects.push(OSMObject::Node(node)),
                                    Err(err) => {
                                        error!(
                                            "unable to read node element {:?}, utf8 error {:?}",
                                            &e, err
                                        );
                                    }
                                }
                            } else if name == QName(b"way") {
                                let way = Way::new_from_element(&mut data, &e);
                                match way {
                                    Ok(way) => deleted_objects.push(OSMObject::Way(way)),
                                    Err(err) => {
                                        error!(
                                            "unable to read way element {:?}, utf8 error {:?}",
                                            &e, err
                                        );
                                    }
                                }
                            } else if name == QName(b"relation") {
                                let relation = Relation::new_from_element(&mut data, &e);
                                match relation {
                                    Ok(relation) => {
                                        deleted_objects.push(OSMObject::Relation(relation))
                                    }
                                    Err(err) => {
                                        error!(
                                            "unable to read relation element {:?}, utf8 error {:?}",
                                            &e, err
                                        );
                                    }
                                }
                            } else {
                                warn!("Unexpected tag: {:?}", name);
                                data.read_to_end(name)?;
                            }
                        } else {
                            if let Event::Text(ref text) = event {
                                if text.borrow().starts_with(b"\n") {
                                    continue;
                                }
                            }
                            warn!("Unexpected event in create: {:?}", event);
                            // Write the data to file for debugging

                            let mut file = std::fs::File::create("debug.xml")?;
                            file.write_all(file_data.as_bytes())?;
                            file.sync_all()?;
                        }
                        skip_buf = Vec::new();
                    }

                    // write the objects to the git repo as yaml files
                    let repository_folder = repository.path().parent().unwrap();
                    for object in deleted_objects {
                        let object_file_name = match object {
                            OSMObject::Node(ref node) => format!("{}.yaml", node.id),
                            OSMObject::Way(ref way) => format!("{}.yaml", way.id),
                            OSMObject::Relation(ref relation) => format!("{}.yaml", relation.id),
                        };
                        let object_file_path = repository_folder.join(object_file_name);
                        // Change the file according to the changeset

                        // If we got the file we open it otherwise we create a new object
                        if !object_file_path.exists() {
                            // We need to create the file
                            let object_file = OpenOptions::new()
                                .read(true)
                                .write(true)
                                .create(true)
                                .open(&object_file_path)?;
                            serde_yaml::to_writer(object_file, &object)?;
                        }
                        let mut object_file =
                            OpenOptions::new().read(true).open(&object_file_path)?;

                        let mut file_object: OSMObject = serde_yaml::from_reader(&mut object_file)?;

                        match object {
                            OSMObject::Node(ref node) => {
                                if let OSMObject::Node(ref mut file_node) = file_object {
                                    file_node.changeset = node.changeset;
                                    file_node.file_generator = node.file_generator.clone();
                                    file_node.file_version = node.file_version.clone();
                                    file_node.legacy_object_version =
                                        node.legacy_object_version.clone();
                                    file_node.lat = node.lat;
                                    file_node.lon = node.lon;
                                    file_node.tags = node.tags.clone();
                                }
                            }
                            OSMObject::Way(ref way) => {
                                if let OSMObject::Way(ref mut file_way) = file_object {
                                    file_way.changeset = way.changeset;
                                    file_way.file_generator = way.file_generator.clone();
                                    file_way.file_version = way.file_version.clone();
                                    file_way.legacy_object_version =
                                        way.legacy_object_version.clone();
                                    file_way.tags = way.tags.clone();
                                    file_way.nodes = way.nodes.clone();
                                }
                            }
                            OSMObject::Relation(ref relation) => {
                                if let OSMObject::Relation(ref mut file_relation) = file_object {
                                    file_relation.changeset = relation.changeset;
                                    file_relation.file_generator = relation.file_generator.clone();
                                    file_relation.file_version = relation.file_version.clone();
                                    file_relation.legacy_object_version =
                                        relation.legacy_object_version.clone();
                                    file_relation.tags = relation.tags.clone();
                                    file_relation.member = relation.member.clone();
                                }
                            }
                        }
                        let object_file = OpenOptions::new()
                            .read(true)
                            .write(true)
                            .truncate(true)
                            .open(object_file_path)?;
                        serde_yaml::to_writer(object_file, &object)?;
                        // Add the object to the list of created objects for the changeset based on the changeset id
                        let changeset = match object {
                            OSMObject::Node(ref node) => node.changeset,
                            OSMObject::Way(ref way) => way.changeset,
                            OSMObject::Relation(ref relation) => relation.changeset,
                        };

                        created_or_modified_objects_for_changeset
                            .entry(changeset)
                            .or_insert_with(Vec::new)
                            .push(object);
                    }
                }
                b"delete" => {
                    // TODO: What do we do in case of an error?

                    let mut deleted_objects = Vec::new();

                    loop {
                        let event = data.read_event_into(&mut skip_buf)?;

                        if let Event::End(ref e) = event {
                            if e.name() == element.name() {
                                break;
                            }
                        }

                        if let Event::Start(ref e) = event {
                            let name = e.name();
                            if name == QName(b"node") {
                                let node = Node::new_from_element(&mut data, &e);
                                match node {
                                    Ok(node) => deleted_objects.push(OSMObject::Node(node)),
                                    Err(err) => {
                                        error!(
                                            "unable to read node element {:?}, utf8 error {:?}",
                                            &e, err
                                        );
                                    }
                                }
                            } else if name == QName(b"way") {
                                let way = Way::new_from_element(&mut data, &e);
                                match way {
                                    Ok(way) => deleted_objects.push(OSMObject::Way(way)),
                                    Err(err) => {
                                        error!(
                                            "unable to read way element {:?}, utf8 error {:?}",
                                            &e, err
                                        );
                                    }
                                }
                            } else if name == QName(b"relation") {
                                let relation = Relation::new_from_element(&mut data, &e);
                                match relation {
                                    Ok(relation) => {
                                        deleted_objects.push(OSMObject::Relation(relation))
                                    }
                                    Err(err) => {
                                        error!(
                                            "unable to read relation element {:?}, utf8 error {:?}",
                                            &e, err
                                        );
                                    }
                                }
                            } else {
                                warn!("Unexpected tag: {:?}", name);
                                data.read_to_end(name)?;
                            }
                        } else {
                            if let Event::Text(ref text) = event {
                                if text.borrow().starts_with(b"\n") {
                                    continue;
                                }
                            }
                            warn!("Unexpected event in create: {:?}", event);
                            // Write the data to file for debugging

                            let mut file = std::fs::File::create("debug.xml")?;
                            file.write_all(file_data.as_bytes())?;
                            file.sync_all()?;
                        }
                        skip_buf = Vec::new();
                    }

                    // write the objects to the git repo as yaml files
                    let repository_folder = repository.path().parent().unwrap();
                    for object in deleted_objects {
                        let object_file_name = match object {
                            OSMObject::Node(ref node) => format!("{}.yaml", node.id),
                            OSMObject::Way(ref way) => format!("{}.yaml", way.id),
                            OSMObject::Relation(ref relation) => format!("{}.yaml", relation.id),
                        };
                        let object_file_path = repository_folder.join(object_file_name);

                        // Delete the file if it exists
                        if object_file_path.exists() {
                            std::fs::remove_file(object_file_path)?;
                        }

                        // Add the object to the list of created objects for the changeset based on the changeset id
                        let changeset = match object {
                            OSMObject::Node(ref node) => node.changeset,
                            OSMObject::Way(ref way) => way.changeset,
                            OSMObject::Relation(ref relation) => relation.changeset,
                        };
                        deleted_objects_for_changeset
                            .entry(changeset)
                            .or_insert_with(Vec::new)
                            .push(object.clone());
                        // Remove it from the list of created objects if it exists
                        if let Some(index) = created_or_modified_objects_for_changeset
                            .get_mut(&changeset)
                            .and_then(|objects| {
                                objects
                                    .iter()
                                    .position(|existing_object| *existing_object == object)
                            })
                        {
                            created_or_modified_objects_for_changeset
                                .get_mut(&changeset)
                                .unwrap()
                                .remove(index);
                        }
                    }
                }
                _ => (),
            },
            Event::Eof => break, // exits the loop when reaching end of file
            _ => (),             // There are `Event` types not considered here
        }
        buf = Vec::new();
    }

    // For all the objects changed apply the changesets as commits
    // Get changeset list from BTreeMaps
    let changeset_list: Vec<u64> = created_or_modified_objects_for_changeset
        .keys()
        .chain(deleted_objects_for_changeset.keys())
        .copied()
        .collect();

    // Find latest changeset file (highest number in filename after "changesets-" and before ".osm.zst")
    let changeset_files = std::fs::read_dir(changesets_location)?;
    let mut last_highest_id = 0;
    let mut changeset_path = String::new();
    for changeset_file in changeset_files {
        let changeset_file = changeset_file?;
        let changeset_file_path = changeset_file.path();
        let changeset_file_name = changeset_file_path.file_name().unwrap().to_str().unwrap();
        let changeset_file_name = changeset_file_name.trim_end_matches(".osm.zst");
        let changeset_file_name = changeset_file_name.trim_start_matches("changesets-");
        let changeset_file_name = changeset_file_name.parse::<u64>();
        if let Ok(changeset_file_name) = changeset_file_name {
            if changeset_file_name > last_highest_id {
                last_highest_id = changeset_file_name;
                changeset_path = changeset_file_path.to_str().unwrap().to_string();
            }
        }
    }

    let changeset_file = File::open(changeset_path)?;
    let mut uncompressed_data = uncompress_changeset_file(changeset_file);

    let changesets = parse_changeset(&mut uncompressed_data, &changeset_list)?;

    for changeset_id in changeset_list {
        // Find the changeset within the files of the cache
        let changeset = find_changesets_in_cache(&changesets, changeset_id)?;

        if changeset.is_none() {
            warn!("Unable to find changeset {:?}", changeset_id);
            continue;
        }

        if let Some(changeset) = changeset {
            // Get comment tag if it exists and trim it
            let comment = changeset
                .tags
                .get("comment")
                .map(|s| s.trim())
                .unwrap_or("");

            // Parse changeset time (ISO 8601) to git time (seconds since epoch) with offset 0 (UTC) using `time`
            let changeset_time = changeset
                .closed_at
                .clone()
                .unwrap_or(changeset.created_at.clone());
            let commit_time =
                OffsetDateTime::parse(changeset_time.as_str(), &Iso8601::DEFAULT)?.unix_timestamp();

            let author = git2::Signature::new(
                &changeset.user,
                &format!("{}@osm", changeset.user),
                &Time::new(commit_time, 0),
            )
            .expect("Unable to create author signature");

            let repository_folder = repository.path().parent().unwrap();

            let added_or_changed_files = created_or_modified_objects_for_changeset
                .get(&changeset.id)
                .unwrap_or(&Vec::new())
                .iter()
                .map(|object| match object {
                    OSMObject::Node(ref node) => {
                        repository_folder.join(format!("{}.yaml", node.id))
                    }
                    OSMObject::Way(ref way) => repository_folder.join(format!("{}.yaml", way.id)),
                    OSMObject::Relation(ref relation) => {
                        repository_folder.join(format!("{}.yaml", relation.id))
                    }
                })
                .map(|path| path.to_string_lossy().to_string())
                .collect::<Vec<String>>();

            let removed_files = deleted_objects_for_changeset
                .get(&changeset.id)
                .unwrap_or(&Vec::new())
                .iter()
                .map(|object| match object {
                    OSMObject::Node(ref node) => {
                        repository_folder.join(format!("{}.yaml", node.id))
                    }
                    OSMObject::Way(ref way) => repository_folder.join(format!("{}.yaml", way.id)),
                    OSMObject::Relation(ref relation) => {
                        repository_folder.join(format!("{}.yaml", relation.id))
                    }
                })
                .map(|path| path.to_string_lossy().to_string())
                .collect::<Vec<String>>();

            let oid = commit(
                repository,
                added_or_changed_files,
                removed_files,
                comment,
                &author,
                committer,
            )?;

            // Convert tags to "Key: Value" strings separated by newlines for the note
            let note = changeset
                .tags
                .iter()
                .filter_map(|(key, value)| {
                    if key.trim().is_empty() {
                        None
                    } else {
                        Some((key, value))
                    }
                })
                .map(|(key, value)| format!("{}: {}", key, value))
                .collect::<Vec<String>>()
                .join("\n");

            // Add the id of the changeset to the note
            let note = if note.is_empty() {
                format!("Legacy Changeset ID: {}", changeset.id)
            } else {
                format!("Legacy Changeset ID: {}\n{}", changeset.id, note)
            };

            repository.note(&author, committer, None, oid, &note, false)?;
        }
    }

    Ok(())
}

/// Scans the files in the cache folder and returns the requested changeset
///
/// # Arguments
///
/// * `cache_folder` - The folder where the changesets are stored
/// * `changeset_id` - The id of the changeset to find
///
/// # Returns
///
/// The changeset if found
fn find_changesets_in_cache(
    changesets: &[Changeset],
    changeset_id: u64,
) -> Result<Option<&Changeset>> {
    let mut changeset = None;

    if changesets.iter().any(|c| c.id == changeset_id) {
        changeset = changesets.iter().find(|c| c.id == changeset_id);
    }

    Ok(changeset)
}
