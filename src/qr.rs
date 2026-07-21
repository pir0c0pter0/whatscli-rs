use qrcode::{QrCode, render::unicode};

pub fn render(payload: &str) -> Result<String, qrcode::types::QrError> {
    Ok(QrCode::new(payload.as_bytes())?
        .render::<unicode::Dense1x2>()
        .quiet_zone(true)
        .dark_color(unicode::Dense1x2::Dark)
        .light_color(unicode::Dense1x2::Light)
        .build())
}
