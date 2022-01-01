use async_std::{
    channel::{bounded, Receiver, Sender},
    fs::File,
    io::{prelude::BufReadExt, BufReader},
    sync::Arc,
};
use encoding_rs::GBK;
use xlsxwriter::{Workbook, XlsxError};

#[async_std::main]
async fn main() -> color_eyre::Result<()> {
    tracing_subscriber::fmt::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    color_eyre::install()?;

    let output_name = "output.xlsx";
    let (s, r) = bounded(64);
    let sender = Arc::new(s);
    let receiver = Arc::new(r);
    let mut sender_waiter = vec![];
    let mut counter = 0;

    for arg in std::env::args().skip(1) {
        sender_waiter.push(async_std::task::spawn(read_from_htsc_export_txt(
            arg.clone(),
            sender.clone(),
        )));
        counter += 1;
    }

    write_htsc_to_tzzb_excel(output_name.to_owned(), receiver.clone(), counter).await?;
    Ok(())
}

async fn read_from_htsc_export_txt(
    path: String,
    sender: Arc<Sender<Option<DeliveryOrder>>>,
) -> std::io::Result<()> {
    let mut reader = BufReader::new(File::open(&path).await?);
    let mut buffer = Vec::with_capacity(4096);
    let gbk_encoder = GBK;

    if reader.read_until(0x0a as u8, &mut buffer).await? > 0 {
        let (line, _, _) = gbk_encoder.decode(&buffer);
        let title: Vec<String> = line.trim().split("\t").map(|v| String::from(v)).collect();
        // checking the line name
        for i in 0..HTSC_CHECK_TITLE.len() {
            assert_eq!(HTSC_CHECK_TITLE[i], title[i]);
        }
    }

    loop {
        buffer.clear();

        let size = reader.read_until(0x0a as u8, &mut buffer).await?;

        if size > 0 {
            let (line, _, _) = gbk_encoder.decode(&buffer);
            let order = DeliveryOrder::from_htsc_line(line.to_string());

            sender
                .send(Some(order))
                .await
                .expect(&format!("Can't send data from read thread: {}!", &path));
        } else {
            sender
                .send(None)
                .await
                .expect(&format!("Can't send data to write thread"));
            break;
        }
    }

    Ok(())
}

async fn write_htsc_to_tzzb_excel(
    path: String,
    rec: Arc<Receiver<Option<DeliveryOrder>>>,
    read_thread_counter: i32,
) -> Result<(), XlsxError> {
    static TZZB_TITLE: [&'static str; 8] = [
        "成交日期",
        "证券代码",
        "证券名称",
        "交易类别",
        "成交数量",
        "成交价格",
        "发生金额",
        "证券余额",
    ];

    let workbook = Workbook::new(&path);
    let mut sheet = workbook.add_worksheet(None)?;
    let mut counter = 0;
    let mut read_stop_counter = 0;

    for idx in 0..TZZB_TITLE.len() {
        sheet.write_string(counter, idx as u16, TZZB_TITLE[idx], None)?;
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
            sheet.write_string(counter, 6, order.get_actual(), None)?;
            sheet.write_string(counter, 7, order.get_owned(), None)?;
        } else {
            read_stop_counter += 1;
            if read_stop_counter == read_thread_counter {
                break;
            }
        }
    }

    workbook.close()?;

    Ok(())
}

#[derive(Debug, Default, Clone)]
pub struct DeliveryOrder {
    code: String,
    name: String,
    date: String,
    kind: String,
    count: String,
    prize: String,
    actual: String,
    owned: String,
}

const HTSC_DATE: usize = 0;
const HTSC_CODE: usize = 2;
const HTSC_NAME: usize = 3;
const HTSC_KIND: usize = 4;
const HTSC_COUNT: usize = 5;
const HTSC_PRIZE: usize = 6;
const HTSC_ACTUAL: usize = 11;
const HTSC_OWNED: usize = 18;

static HTSC_CHECK_TITLE: [&'static str; 20] = [
    "发生日期",
    "备注",
    "证券代码",
    "证券名称",
    "买卖标志",
    "成交数量",
    "成交价格",
    "成交金额",
    "佣金",
    "印花税",
    "过户费",
    "发生金额",
    "剩余金额",
    "申报序号",
    "股东代码",
    "席位代码",
    "委托编号",
    "成交编号",
    "证券数量",
    "其他费",
];

impl DeliveryOrder {
    pub fn from_htsc_line(line: String) -> Self {
        let columns: Vec<&str> = line.trim().split("\t").collect();

        assert_eq!(columns.len(), 20);

        Self::default()
            .with_date(columns[HTSC_DATE].to_owned())
            .with_code(columns[HTSC_CODE].to_owned())
            .with_name(columns[HTSC_NAME].to_owned())
            .with_kind(columns[HTSC_KIND].to_owned())
            .with_count(columns[HTSC_COUNT].to_owned())
            .with_prize(columns[HTSC_PRIZE].to_owned())
            .with_actual(columns[HTSC_ACTUAL].to_owned())
            .with_owned(columns[HTSC_OWNED].to_owned())
    }

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

    pub fn set_actual(&mut self, actual: String) {
        self.actual = actual;
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

    pub fn with_actual(mut self, actual: String) -> Self {
        self.actual = actual;
        self
    }

    pub fn with_owned(mut self, owned: String) -> Self {
        self.owned = owned;
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

    pub fn get_actual(&self) -> &String {
        &self.actual
    }

    pub fn get_owned(&self) -> &String {
        &self.owned
    }
}
