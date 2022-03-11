mod htsc;

use std::collections::HashMap;
use std::sync::atomic::AtomicI32;

use aopt::prelude::*;

use async_std::sync::Mutex;
use async_std::{
    channel::{bounded, Receiver},
    sync::Arc,
};

use xlsxwriter::{Workbook, XlsxError};

const HTSC_TYPE: &'static str = "HTSC";
const OUTPUT: &'static str = "output.xlsx";

#[async_std::main]
async fn main() -> color_eyre::Result<()> {
    tracing_subscriber::fmt::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    color_eyre::install()?;

    let (s, r) = bounded(128);
    let sender = Arc::new(s);
    let receiver = Arc::new(r);
    let htsc_context = Arc::new(Mutex::new(htsc::Context::new()));
    let mut parser = ForwardParser::default();

    parser
        .add_opt("-t=s!")?
        .add_alias("--type")?
        .set_default_value(HTSC_TYPE.into())
        .commit()?;
    parser
        .add_opt("-o=s")?
        .add_alias("--output")?
        .set_default_value(OUTPUT.into())
        .commit()?;
    parser.add_opt("-d=b")?.add_alias("--debug")?.commit()?;

    let uid = parser.add_opt("input=p!@*")?.commit()?;
    let counter = Arc::new(AtomicI32::new(0));
    let counter_reader = counter.clone();

    type Input = HashMap<String, Vec<String>>;

    parser.add_callback(
        uid,
        simple_pos_mut_cb!(move |uid, set: &mut SimpleSet, path, _, _| {
            let file_type = set["--type"].get_value().as_str().unwrap().clone();
            let opt = set[uid].as_mut();
            let mut inputs: Input;

            if let Some(inner_data) = opt.get_value_mut().downcast_mut::<Input>() {
                inputs = std::mem::take(inner_data);
            } else {
                inputs = Input::default();
            }
            match file_type.as_str() {
                HTSC_TYPE => {
                    inputs
                        .entry(String::from(file_type))
                        .or_insert(vec![])
                        .push(path.to_owned());
                }
                _ => {
                    panic!("Unknow file type: {}", file_type);
                }
            }
            counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(Some(OptValue::from_any(Box::new(inputs))))
        }),
    );

    getopt!(&mut std::env::args().skip(1), parser)?;

    let inputs;
    let debug = *parser["--debug"].get_value().as_bool().unwrap_or(&false);

    if let Some(inner_data) = parser["input"].get_value_mut().downcast_mut::<Input>() {
        inputs = std::mem::take(inner_data);
    } else {
        inputs = Input::default();
    }
    if debug {
        println!("got file map: {:?}", inputs);
        println!("got output file count = {:?}", counter_reader);
    }
    if counter_reader.load(std::sync::atomic::Ordering::SeqCst) > 0 {
        for (type_, paths) in inputs.iter() {
            match type_.as_str() {
                HTSC_TYPE => {
                    async_std::task::spawn(htsc::extract_from_file(
                        htsc_context.clone(),
                        paths.clone(),
                        sender.clone(),
                        debug,
                    ));
                }
                _ => {}
            }
        }
    }

    let output_name = parser.get_value("--output")?.unwrap().as_str().unwrap();

    if counter_reader.load(std::sync::atomic::Ordering::SeqCst) > 0 {
        if debug {
            println!("got output file name = {:?}", output_name);
        }
        write_htsc_to_tzzb_excel(output_name.to_owned(), receiver.clone(), counter_reader).await?;
    }
    Ok(())
}

async fn write_htsc_to_tzzb_excel(
    path: String,
    rec: Arc<Receiver<Option<DeliveryOrder>>>,
    counter_reader: Arc<AtomicI32>,
) -> Result<(), XlsxError> {
    let title = htsc::Context::gen_title();
    let workbook = Workbook::new(&path);
    let mut sheet = workbook.add_worksheet(None)?;
    let mut counter = 0;
    let mut read_stop_counter = 0;

    for idx in 0..title.len() {
        sheet.write_string(counter, idx as u16, &title[idx], None)?;
    }

    loop {
        if let Some(order) = rec
            .recv()
            .await
            .expect("Unable to receive from read thread")
        {
            counter += 1;
            sheet.write_string(counter, 0, order.get_date(), None)?;
            sheet.write_string(counter, 1, order.get_code(), None)?;
            sheet.write_string(counter, 2, order.get_name(), None)?;
            sheet.write_string(counter, 3, order.get_kind(), None)?;
            sheet.write_string(counter, 4, order.get_count(), None)?;
            sheet.write_string(counter, 5, order.get_prize(), None)?;
            sheet.write_string(counter, 6, order.get_amount(), None)?;
            sheet.write_string(counter, 7, order.get_owned(), None)?;
        } else {
            read_stop_counter += 1;
            if read_stop_counter == counter_reader.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }
        }
    }

    println!("--> read count = {}, {:?}", counter, counter_reader);

    workbook.close()?;

    Ok(())
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum Trade {
    Buy,
    Sell,
    In,
    Out,
    Ignore,
}

impl Default for Trade {
    fn default() -> Self {
        Trade::Ignore
    }
}

#[derive(Debug, Default, Clone)]
pub struct DeliveryOrder {
    code: String,
    name: String,
    date: String,
    kind: String,
    count: String,
    prize: String,
    amount: String,
    owned: String,
    trade: Trade,
}

impl DeliveryOrder {
    pub fn set_code(&mut self, code: String) {
        self.code = code;
    }

    pub fn set_name(&mut self, name: String) {
        self.name = name;
    }

    pub fn set_date(&mut self, date: String) {
        self.date = date;
    }

    pub fn set_kind(&mut self, kind: String) {
        self.kind = kind;
    }

    pub fn set_count(&mut self, count: String) {
        self.count = count;
    }

    pub fn set_prize(&mut self, prize: String) {
        self.prize = prize;
    }

    pub fn set_amount(&mut self, amount: String) {
        self.amount = amount;
    }

    pub fn set_owned(&mut self, owned: String) {
        self.owned = owned;
    }

    pub fn with_code(mut self, code: String) -> Self {
        self.code = code;
        self
    }

    pub fn with_name(mut self, name: String) -> Self {
        self.name = name;
        self
    }

    pub fn with_date(mut self, date: String) -> Self {
        self.date = date;
        self
    }

    pub fn with_kind(mut self, kind: String) -> Self {
        self.kind = kind;
        self
    }

    pub fn with_count(mut self, count: String) -> Self {
        self.count = count;
        self
    }

    pub fn with_prize(mut self, prize: String) -> Self {
        self.prize = prize;
        self
    }

    pub fn with_amount(mut self, amount: String) -> Self {
        self.amount = amount;
        self
    }

    pub fn with_owned(mut self, owned: String) -> Self {
        self.owned = owned;
        self
    }

    pub fn with_trade(mut self, trade: Trade) -> Self {
        self.trade = trade;
        self
    }

    pub fn get_code(&self) -> &String {
        &self.code
    }

    pub fn get_name(&self) -> &String {
        &self.name
    }

    pub fn get_date(&self) -> &String {
        &self.date
    }

    pub fn get_kind(&self) -> &String {
        &self.kind
    }

    pub fn get_count(&self) -> &String {
        &self.count
    }

    pub fn get_prize(&self) -> &String {
        &self.prize
    }

    pub fn get_amount(&self) -> &String {
        &self.amount
    }

    pub fn get_owned(&self) -> &String {
        &self.owned
    }

    pub fn get_trade(&self) -> &Trade {
        &self.trade
    }

    pub fn is_valid(&self) -> bool {
        match self.trade {
            Trade::Ignore => false,
            _ => true,
        }
    }
}
