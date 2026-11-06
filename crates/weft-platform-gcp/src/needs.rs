//! What a node needs, on Google Cloud: a worker is an image, and its
//! build installs every need it has packages for (for the base image's
//! package manager and distro), on the install line of the need's stage.
//! What a program keeps of its nodes' run needs is only what its image
//! left to the machine (`weft_compiler::build::StagedImageBuild::node_needs`):
//! a need with no packages for that image, or one a custom Dockerfile
//! template gives no install line to. A cloud machine runs nothing beside
//! the image, so every one of those is missing and the program is refused
//! before it starts.

use async_trait::async_trait;
use weft_core::needs::Need;
use weft_platform_traits::MachineNeeds;

pub struct ImageNeeds;

#[async_trait]
impl MachineNeeds for ImageNeeds {
    async fn unmet(&self, needs: &[Need]) -> anyhow::Result<Vec<Need>> {
        Ok(needs.to_vec())
    }

    fn needs_from(&self) -> weft_core::needs::NeedsFrom {
        weft_core::needs::NeedsFrom::Image
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn every_need_the_image_left_to_the_machine_is_missing() {
        let asked = [weft_core::needs::NodeNeed {
            node_type: "ExecPython".into(),
            nodes: vec!["greet".into()],
            need: weft_core::needs::Need { name: "xcode".into(), stage: Default::default(), check: vec!["xcode".into()], install: Default::default(), how: None, recipe: None },
        }];
        assert_eq!(ImageNeeds.missing(&asked).await.unwrap(), asked.to_vec());
    }
}
