use color_eyre::eyre::Result;
use flate2::bufread::GzDecoder;
use quick_xml::{
    events::{BytesStart, Event},
    name::QName,
    Reader,
};
use std::{
    borrow::Cow,
    collections::HashMap,
    convert::Infallible,
    io::{Read, Write},
};
use tracing::{debug, error, info, warn};

#[derive(Debug, Clone, PartialEq)]
pub struct Changeset {
    pub id: u64,
    pub created_at: String,
    pub closed_at: Option<String>,
    pub open: bool,
    pub user: String,
    pub uid: u64,
    pub min_lat: Option<f64>,
    pub max_lat: Option<f64>,
    pub min_lon: Option<f64>,
    pub max_lon: Option<f64>,
    pub tags: HashMap<String, String>,
}

impl Changeset {
    fn new_from_element(reader: &mut Reader<&[u8]>, element: BytesStart) -> Result<Self> {
        let changeset_attributes: HashMap<String, String> = element
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

        let mut changeset = Changeset {
            id: changeset_attributes.get("id").unwrap().parse().unwrap(),
            created_at: changeset_attributes.get("created_at").unwrap().to_string(),
            closed_at: changeset_attributes.get("closed_at").map(|s| s.to_string()),
            open: changeset_attributes.get("open").unwrap().parse().unwrap(),
            user: changeset_attributes.get("user").unwrap().to_string(),
            uid: changeset_attributes.get("uid").unwrap().parse().unwrap(),
            min_lat: changeset_attributes
                .get("min_lat")
                .map(|s| s.parse().unwrap()),
            max_lat: changeset_attributes
                .get("max_lat")
                .map(|s| s.parse().unwrap()),
            min_lon: changeset_attributes
                .get("min_lon")
                .map(|s| s.parse().unwrap()),
            max_lon: changeset_attributes
                .get("max_lon")
                .map(|s| s.parse().unwrap()),
            tags: HashMap::new(),
        };

        let mut element_buf = Vec::new();
        loop {
            let event = reader.read_event_into(&mut element_buf)?;

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

                    for attr_result in element.attributes() {
                        let a = attr_result?;
                        match a.key.as_ref() {
                            b"k" => key = a.decode_and_unescape_value(reader)?,
                            b"v" => value = a.decode_and_unescape_value(reader)?,
                            _ => (),
                        }
                    }

                    changeset.tags.insert(key.to_string(), value.to_string());
                } else {
                    warn!("Unexpected tag: {:?}", name);
                    reader.read_to_end(name)?;
                }
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
                warn!("Unexpected event in changeset: {:?}", event);
                // Write the data to file for debugging

                let mut file = std::fs::File::create("debug.xml")?;
                file.write_all(&element_buf)?;
                file.sync_all()?;
            }
        }
        Ok(changeset)
    }
}

pub fn parse_changeset(changeset_data: &[u8]) -> Result<Vec<Changeset>> {
    // If file is empty, return an empty vector
    if changeset_data.is_empty() {
        return Ok(Vec::new());
    }
    // Decompress the changeset file
    let mut changeset_data_reader = GzDecoder::new(changeset_data);
    let mut changeset_data = String::new();
    changeset_data_reader.read_to_string(&mut changeset_data)?;
    debug!(
        "Changeset file decompressed. Size: {}",
        changeset_data.len()
    );

    // If the changeset file is empty, return an empty vector
    if changeset_data.is_empty() {
        return Ok(Vec::new());
    }

    let mut changeset_data = Reader::from_str(&changeset_data);

    // == Handling empty elements ==
    // To simply our processing code
    // we want the same events for empty elements, like:
    //   <DefaultSettings Language="es" Greeting="HELLO"/>
    //   <Text/>
    changeset_data.expand_empty_elements(true);

    let mut changesets = Vec::new();
    let mut buf = Vec::new();

    // Parse the changeset file
    info!("Parsing changeset file");
    loop {
        let event = changeset_data.read_event_into(&mut buf)?;
        match event {
            Event::Start(element) => {
                if let b"changeset" = element.name().as_ref() {
                    // TODO: What do we do in case of an error?
                    let changeset =
                        Changeset::new_from_element(&mut changeset_data, element.clone());
                    match changeset {
                        Ok(changeset) => changesets.push(changeset),
                        Err(err) => {
                            error!(
                                "unable to read changeset element {:?}, utf8 error {:?}",
                                &element, err
                            );
                        }
                    }
                }
            }
            Event::Eof => break, // exits the loop when reaching end of file
            _ => (),             // There are `Event` types not considered here
        }
    }
    Ok(changesets)
}
