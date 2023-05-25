use color_eyre::eyre::Result;
use quick_xml::{
    events::{BytesStart, Event},
    name::QName,
    Reader,
};
use std::{
    borrow::Cow,
    collections::{BTreeSet, HashMap},
    convert::Infallible,
    fs::File,
    io::{BufReader, Write},
};
use tracing::{debug, error, info, warn};
use zstd::stream::Decoder;

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
    fn new_from_element(
        reader: &mut Reader<BufReader<Decoder<'_, BufReader<File>>>>,
        element: &BytesStart,
        changeset_list: &[u64],
    ) -> Result<Option<Self>> {
        let changeset_attributes: HashMap<String, String> = element
            .attributes()
            .filter_map(|attr_result| attr_result.ok())
            .map(|attr| {
                let key = reader
                    .decoder()
                    .decode(attr.key.local_name().as_ref())
                    .or_else(|err| {
                        debug!(
                            "unable to read key in DefaultSettings attribute {:?}, utf8 error {:?}",
                            &attr, err
                        );
                        Ok::<Cow<'_, str>, Infallible>(std::borrow::Cow::from(""))
                    })
                    .unwrap()
                    .to_string();
                let value = attr
                    .decode_and_unescape_value(reader)
                    .or_else(|err| {
                        debug!(
                            "unable to read key in DefaultSettings attribute {:?}, utf8 error {:?}",
                            &attr, err
                        );
                        Ok::<Cow<'_, str>, Infallible>(std::borrow::Cow::from(""))
                    })
                    .unwrap()
                    .to_string();
                (key, value)
            })
            .collect();

        //debug!("changeset_attributes: {:?}", changeset_attributes);

        let id = changeset_attributes.get("id").unwrap().parse().unwrap();
        if !changeset_list.contains(&id) {
            return Ok(None);
        }

        let mut changeset = Changeset {
            id: changeset_attributes.get("id").unwrap().parse().unwrap(),
            created_at: changeset_attributes.get("created_at").unwrap().to_string(),
            closed_at: changeset_attributes.get("closed_at").map(|s| s.to_string()),
            open: changeset_attributes.get("open").unwrap().parse().unwrap(),
            user: changeset_attributes
                .get("user")
                .map(|s| s.to_string())
                .unwrap_or_else(|| "Unknown".to_string()),
            uid: changeset_attributes
                .get("uid")
                .unwrap_or(&"0".to_string())
                .parse()
                .unwrap(),
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

        let mut new_buf = Vec::new();

        loop {
            let event = reader.read_event_into(&mut new_buf)?;

            match event {
                Event::End(ref e) => {
                    if e.name() == element.name() {
                        break;
                    }
                }
                Event::Start(ref e) => {
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
                        //reader.read_to_end_into(e.name(), &mut new_buf);
                    }
                }
                _ => {
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
                    file.write_all(&new_buf)?;
                    file.sync_all()?;
                }
            }
            new_buf = Vec::new();
        }

        Ok(Some(changeset))
    }
}

pub fn uncompress_changeset_file<'a>(
    file: File,
) -> Reader<BufReader<Decoder<'a, BufReader<File>>>> {
    // Decompress the changeset file
    info!("Decompressing changeset file");
    let reader: BufReader<Decoder<BufReader<File>>> = BufReader::new(Decoder::new(file).unwrap());
    Reader::from_reader(reader)
}

pub fn parse_changeset(
    changeset_data: &mut Reader<BufReader<Decoder<'_, BufReader<File>>>>,
    changeset_list: &[u64],
) -> Result<Vec<Changeset>> {
    // == Handling empty elements ==
    // To simply our processing code
    // we want the same events for empty elements, like:
    //   <DefaultSettings Language="es" Greeting="HELLO"/>
    //   <Text/>
    changeset_data.expand_empty_elements(true);

    let mut changesets = Vec::new();
    let changeset_hashset = changeset_list.iter().cloned().collect::<BTreeSet<u64>>();
    let mut buf = Vec::new();

    // Parse the changeset file
    info!("Parsing changeset file");
    loop {
        // If we already have all of them then break
        // We compare the ids even if its a little more expensive
        let ids_parsed = changesets
            .iter()
            .map(|c: &Changeset| c.id)
            .collect::<BTreeSet<u64>>();
        if changeset_hashset.is_subset(&ids_parsed) {
            break;
        }

        let event = changeset_data.read_event_into(&mut buf)?;
        match event {
            Event::Start(element) => {
                if let b"changeset" = element.name().as_ref() {
                    // TODO: What do we do in case of an error?
                    let changeset =
                        Changeset::new_from_element(changeset_data, &element, changeset_list);

                    match changeset {
                        Ok(Some(changeset)) => {
                            changesets.push(changeset);
                        }
                        Err(err) => {
                            error!(
                                "unable to read changeset element {:?}, utf8 error {:?}",
                                &element, err
                            );
                        }
                        _ => {}
                    }
                }
                //changeset_data.read_to_end_into(element.name(), &mut buf);
            }
            Event::Eof => break, // exits the loop when reaching end of file
            _ => (),             // There are `Event` types not considered here
        }
        buf = Vec::new();
    }
    Ok(changesets)
}
